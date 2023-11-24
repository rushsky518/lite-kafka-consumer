package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * kafka consumer poll thread, and submit tasks on batch
 */
public class BatchKafkaPollThread<K, V> extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchKafkaPollThread.class);

    private KafkaConsumer<K,V> kafkaConsumer;
    private KafkaWorker kafkaWorker;
    // poll 线程可运行标识
    private volatile boolean stop = false;
    private OffsetMgr offsetMgr = null;
    private Set<TopicPartition> partitions = null;
    private int batchSize = 4;
    private BatchTaskGenerator<K, V> taskGenerator;

    private String groupId;
    protected volatile ResetOps resetOps;

    public BatchKafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, BatchTaskGenerator<K, V> kvTaskGenerator, String name) {
        this(kafkaConsumer, kvTaskGenerator, name, new SequentialThread());
    }

    public BatchKafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, BatchTaskGenerator<K, V> kvTaskGenerator, String name, KafkaWorker kafkaWorker) {
        this.kafkaConsumer = kafkaConsumer;
        this.taskGenerator = kvTaskGenerator;
        this.setName(name);
        this.kafkaWorker = kafkaWorker;
        this.groupId = kafkaConsumer.groupMetadata().groupId();
    }

    @Override
    public void run() {
        long lastPollTime = 0;
        List<ConsumerRecord<K, V>> batch = new ArrayList<>(batchSize);

        while (!this.stop) {
            try {
                resetIfNeed();

                if (offsetMgr != null) {
                    if (offsetMgr.isAllConsumed()) {
                        kafkaConsumer.resume(partitions);
                        kafkaConsumer.commitSync();
                        offsetMgr = null;
                    } else {
                        // 'max.poll.interval.ms' default value is 300000
                        if (System.currentTimeMillis() - lastPollTime > 200_000L) {
                            ConsumerRecords<K, V> discard = kafkaConsumer.poll(Duration.ofSeconds(1));
                            LOGGER.warn("A redundant poll is done to avoid re-balance, {}", discard);
                            lastPollTime = System.currentTimeMillis();
                            // poll once to avoid re-balance, just discard records
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
                lastPollTime = System.currentTimeMillis();
                offsetMgr = OffsetMgr.get(records);

                for (ConsumerRecord<K, V> record : records) {
                    batch.add(record);
                    if (batch.size() == batchSize) {
                        BatchKafkaTask<K, V> batchKafkaTask = taskGenerator.decorate();
                        batchKafkaTask.records = new ArrayList<>(batch);
                        batchKafkaTask.offsetMgr = offsetMgr;
                        batch.clear();
                        kafkaWorker.submit(batchKafkaTask);
                    }
                }
                // 处理剩余消息
                if (batch.size() > 0) {
                    BatchKafkaTask<K, V> batchKafkaTask = taskGenerator.decorate();
                    batchKafkaTask.records = new ArrayList<>(batch);
                    batchKafkaTask.offsetMgr = offsetMgr;
                    batch.clear();
                    kafkaWorker.submit(batchKafkaTask);
                }

                // 等待这批消息消费完成
                partitions = records.partitions();
                kafkaConsumer.pause(partitions);
            } catch (Exception ex) {
                LOGGER.error("poll error", ex);
                offsetMgr = null;
            }
        }

        kafkaConsumer.close();
    }

    public void stopPoll() {
        this.stop = true;
        this.kafkaWorker.shutdown();
    }
    protected String groupId() {
        return this.groupId;
    }

    private void resetIfNeed() {
        if (resetOps != null) {
            Set assignment = kafkaConsumer.assignment();
            TopicPartition tp = new TopicPartition(resetOps.getTopic(), resetOps.getPartition());
            if (assignment.contains(tp)) {
                kafkaConsumer.seek(tp, resetOps.getOffset());
            }
            resetOps = null;
        }
    }
}
