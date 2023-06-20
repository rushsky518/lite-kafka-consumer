package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * 消息的简单封装
 */
public abstract class KafkaTask<K ,V> implements Runnable, Consumer<ConsumerRecord<K ,V>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTask.class);
    // 重试次数
    private static int RETRY_LIMIT = 1;

    protected ConsumerRecord<K, V> record;
    protected OffsetMgr offsetMgr;

    public KafkaTask(ConsumerRecord<K, V> record, OffsetMgr offsetMgr) {
        this.record = record;
        this.offsetMgr = offsetMgr;
    }

    @Override
    public void run() {
        int retry = 0;
        boolean committed = false;
        while (retry < RETRY_LIMIT) {
            try {
                accept(record);
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
        offsetMgr.commit(record.topic(), record.partition(), record.offset());
    }

    protected ConsumerRecord<K, V> getRecord() {
        return record;
    }
}
