package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * kafka consumer poll thread, and submit task one by one.
 * make sure KafkaConsumer's autocommit is disabled.
 */
public class KafkaPollThread<K, V> extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPollThread.class);

    private KafkaConsumer<K,V> kafkaConsumer;
    private KafkaWorker kafkaWorker;
    private boolean addStopHook = false;
    // poll 线程可运行标识
    private volatile boolean stop = false;
    // period millis to poll messages, default -1
    private long pollPeriod = -1L;
    // interval millis to commit offset, will cause data inconsistency, not suggested
    private long commitInterval = -1L;
    private OffsetMgr offsetMgr = null;
    private Set<TopicPartition> partitions = null;
    private boolean commitWhenClose = false;
    private TaskGenerator<K, V> taskGenerator;

    private String groupId;
    protected ArrayBlockingQueue<ResetOps> resetQueue = new ArrayBlockingQueue<>(16);

    public KafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, TaskGenerator<K, V> kvTaskGenerator, String name) {
        this(kafkaConsumer, kvTaskGenerator, name, new SequentialThread());
    }

    public KafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, TaskGenerator<K, V> kvTaskGenerator, String name, KafkaWorker kafkaWorker) {
        this.kafkaConsumer = kafkaConsumer;
        this.taskGenerator = kvTaskGenerator;
        this.setName(name);
        this.kafkaWorker = kafkaWorker;
        this.groupId = kafkaConsumer.groupMetadata().groupId();
    }

    long lastPollTime = 0;
    long lastCommitTime = 0;

    @Override
    public void run() {
        if (addStopHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> stopPoll()));
        }

        while (!this.stop) {
            try {
                resetIfNeed();

                if (offsetMgr != null) {
                    if (offsetMgr.isAllConsumed()) {
                        if (canResume()) {
                            kafkaConsumer.resume(partitions);
                            if (canCommit()) {
                                kafkaConsumer.commitSync();
                                lastCommitTime = System.nanoTime();
                            }
                            offsetMgr = null;
                        } else {
                            LOGGER.debug("wait {} ms to resume", pollPeriod);
                            if (nanoToMillis(System.nanoTime() - lastPollTime) > 200_000L) {
                                emptyPoll();
                            }
                            TimeUnit.MILLISECONDS.sleep(pollPeriod > 0L ? pollPeriod : 10L);
                            continue;
                        }
                    } else {
                        // 'max.poll.interval.ms' default value is 300000
                        if (nanoToMillis(System.nanoTime() - lastPollTime) > 200_000L) {
                            emptyPoll();
                        } else {
                            offsetMgr.waitAllConsumed(50, TimeUnit.MILLISECONDS);
                        }
                        continue;
                    }
                }

                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    continue;
                }

                // 记录当前时间
                lastPollTime = System.nanoTime();
                offsetMgr = OffsetMgr.get(records);
                for (ConsumerRecord<K, V> record : records) {
                    if (taskGenerator != null) {
                        KafkaTask<K, V> task = taskGenerator.decorate();
                        task.record = record;
                        task.offsetMgr = offsetMgr;
                        if (kafkaWorker != null) {
                            kafkaWorker.submit(task);
                        } else {
                            LOGGER.warn("kafkaWorker is null");
                        }
                    }
                }

                // 等待这批消息消费完成
                partitions = kafkaConsumer.assignment();
                kafkaConsumer.pause(partitions);
            } catch (Exception ex) {
                LOGGER.error("poll error", ex);
                offsetMgr = null;

                if (ex instanceof RebalanceInProgressException) {
                    kafkaWorker.refresh();
                }
            }
        }
    }

    private void emptyPoll() {
        ConsumerRecords<K, V> discard = kafkaConsumer.poll(Duration.ofSeconds(1));
        LOGGER.warn("A redundant poll is done to avoid re-balance, {}", descRecords(discard));
        lastPollTime = System.nanoTime();
    }

    public void setPollPeriod(long pollPeriod) {
        this.pollPeriod = pollPeriod;
    }

    public void setCommitInterval(long commitInterval) {
        this.commitInterval = commitInterval;
    }

    private boolean canResume() {
        if (pollPeriod == -1L) {
            return true;
        }
        return nanoToMillis(System.nanoTime() - lastPollTime) > pollPeriod;
    }

    private boolean canCommit() {
        if (commitInterval == -1L) {
            return true;
        }
        return nanoToMillis(System.nanoTime() - lastCommitTime) > commitInterval;
    }

    private long nanoToMillis(long nano) {
        return nano / 1000_000L;
    }

    private String descRecords(ConsumerRecords<K, V> discard) {
        StringBuilder sb = new StringBuilder();

        for (TopicPartition tp: discard.partitions()) {
            List<ConsumerRecord<K, V>> records = discard.records(tp);
            if (records.size() > 0) {
                ConsumerRecord<K, V> record = records.get(0);
                sb.append(tp.toString()).append(":").append(record.offset());
                sb.append(",");
            }
        }

        return sb.toString();
    }

    public void setAddStopHook(boolean addStopHook) {
        this.addStopHook = addStopHook;
    }

    public void setCommitWhenClose(boolean commitWhenClose) {
        this.commitWhenClose = commitWhenClose;
    }

    public void stopPoll() {
        this.stop = true;
        this.interrupt();
        try {
            // wait poll thread to release lock
            TimeUnit.MILLISECONDS.sleep(50);
        } catch (InterruptedException e) {
            // ignore
        }
        this.kafkaWorker.shutdown();
        this.taskGenerator.shutdown();
        if (commitWhenClose) {
            this.kafkaConsumer.commitSync();
        }
        this.kafkaConsumer.close();
        LOGGER.warn("shutdown KafkaPollThread");
    }

    protected String groupId() {
        return this.groupId;
    }

    private void resetIfNeed() {
        ResetOps resetOp;
        while ((resetOp = resetQueue.poll()) != null) {
            Set assignment = kafkaConsumer.assignment();
            TopicPartition tp = new TopicPartition(resetOp.getTopic(), resetOp.getPartition());
            if (assignment.contains(tp)) {
                kafkaConsumer.seek(tp, resetOp.getOffset());
            }
        }
    }
}
