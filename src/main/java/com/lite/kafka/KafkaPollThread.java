package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * kafka consumer poll thread
 */
public class KafkaPollThread<K, V> extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPollThread.class);

    private KafkaConsumer<K,V> kafkaConsumer;
    private KafkaWorker kafkaWorker;
    // poll 线程可运行标识
    private volatile boolean stop = false;
    private OffsetMgr offsetMgr = null;
    private Set<TopicPartition> partitions = null;

    private TaskHandler<K, V> taskHandler;

    private String groupId;
    protected volatile ResetOps resetOps;

    public KafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, TaskHandler<K, V> kvTaskHandler, String name) {
        this(kafkaConsumer, kvTaskHandler, name, new SequentialThread());
    }

    public KafkaPollThread(KafkaConsumer<K, V> kafkaConsumer, TaskHandler<K, V> kvTaskHandler, String name, KafkaWorker kafkaWorker) {
        this.kafkaConsumer = kafkaConsumer;
        this.taskHandler = kvTaskHandler;
        this.setName(name);
        this.kafkaWorker = kafkaWorker;
        this.groupId = kafkaConsumer.groupMetadata().groupId();
    }

    @Override
    public void run() {
        long lastPollTime = 0;
        while (!this.stop) {
            try {
                if (offsetMgr != null) {
                    if (offsetMgr.isAllConsumed()) {
                        kafkaConsumer.resume(partitions);
                        kafkaConsumer.commitSync();
                        offsetMgr = null;
                    } else {
                        if (System.currentTimeMillis() - lastPollTime > 300_000L) {
                            ConsumerRecords<K, V> discard = kafkaConsumer.poll(Duration.ofSeconds(1));
                            // poll once to avoid re-balance, just discard records
                        } else {
                            offsetMgr.waitAllConsumed(50, TimeUnit.MILLISECONDS);
                        }
                        continue;
                    }
                }

                reset();

                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    continue;
                }

                // 记录当前时间
                lastPollTime = System.currentTimeMillis();
                offsetMgr = OffsetMgr.get(records);
                for (ConsumerRecord<K, V> record : records) {
                    if (taskHandler != null) {
                        KafkaTask<K, V> accept = taskHandler.accept(record, offsetMgr);
                        if (kafkaWorker != null) {
                            kafkaWorker.submit(accept);
                        } else {
                            LOGGER.warn("kafkaWorker is null");
                        }
                    }
                }

                // 等待这批消息消费完成
                partitions = records.partitions();
                kafkaConsumer.pause(partitions);
            } catch (Exception ex) {
                LOGGER.error("poll error", ex);
                offsetMgr = null;
            }
        }
    }

    public void stopPoll() {
        this.stop = true;
        this.kafkaConsumer.close();
        this.kafkaWorker.shutdown();
    }
    protected String groupId() {
        return this.groupId;
    }

    private void reset() {
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
