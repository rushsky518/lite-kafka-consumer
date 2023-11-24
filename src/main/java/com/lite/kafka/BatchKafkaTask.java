package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * 一批消息的简单封装
 */
public abstract class BatchKafkaTask<K ,V> implements Runnable, Consumer<List<ConsumerRecord<K ,V>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchKafkaTask.class);
    // 重试次数
    private static int RETRY_LIMIT = 1;

    protected List<ConsumerRecord<K, V>> records;
    protected OffsetMgr offsetMgr;

    public BatchKafkaTask() {}

    public BatchKafkaTask(List<ConsumerRecord<K, V>> records, OffsetMgr offsetMgr) {
        this.records = records;
        this.offsetMgr = offsetMgr;
    }

    @Override
    public void run() {
        int retry = 0;
        boolean committed = false;
        while (retry < RETRY_LIMIT) {
            try {
                accept(records);
                commit();
                committed = true;
                break;
            } catch (Exception ex) {
                LOGGER.error("error", ex);
                retry += 1;
            }
        }
        if (!committed) {
            commit();
        }
    }

    private void commit() {
        for (ConsumerRecord<K, V> record : records) {
            offsetMgr.commit(record.topic(), record.partition(), record.offset());
        }
    }

    protected List<ConsumerRecord<K, V>> getRecords() {
        return records;
    }

}
