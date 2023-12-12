package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * a batch of kafka records
 */
public abstract class BatchKafkaTask<K ,V> implements Runnable, Consumer<List<ConsumerRecord<K ,V>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchKafkaTask.class);
    // 重试次数
    private static int RETRY_LIMIT = 1;

    protected List<ConsumerRecord<K, V>> records;
    protected OffsetMgr offsetMgr;
    protected boolean success = true;

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

        if (!success) {
            onConsumeError();
        }
    }

    private void commit() {
        Map<String, List<Long>> offsetMap = new HashMap<>();

        for (ConsumerRecord<K, V> record : records) {
            String key = record.topic() + "_" + record.partition();
            List<Long> offsets = offsetMap.computeIfAbsent(key, k -> new ArrayList<>());
            offsets.add(record.offset());
        }

        for (Map.Entry<String, List<Long>> entry : offsetMap.entrySet()) {
            List<Long> offsets = entry.getValue();
            offsetMgr.multiCommit(entry.getKey(), offsets);
        }
    }

    protected List<ConsumerRecord<K, V>> getRecords() {
        return records;
    }

    protected void onConsumeError() {}

    protected boolean isSuccess() {
        return success;
    }
}
