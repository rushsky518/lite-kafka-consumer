package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * 对于一批消息的 offset 管理
 */
public class OffsetMgr {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetMgr.class);

    private Map<String, OffsetBits> offsetMap;

    private OffsetMgr(Map<String, OffsetBits> offsetMap) {
        this.offsetMap = offsetMap;
    }

    /**
     * 根据 poll 到的消息创建 offset 管理器
     * @param consumerRecords 消息
     * @return 管理器
     */
    public static OffsetMgr get(ConsumerRecords consumerRecords) {
        Set<TopicPartition> partitions = consumerRecords.partitions();
        Map<String, OffsetBits> offsetMap = new HashMap<>();

        for (TopicPartition tp : partitions) {
            List<ConsumerRecord> recordList = consumerRecords.records(tp);
            ConsumerRecord record = recordList.get(0);
            long base = record.offset();
            int size = recordList.size();
            offsetMap.put(tp.topic() + "_" + tp.partition(), new OffsetBits(base, size));
        }
        LOGGER.debug("poll {} msg(s)", consumerRecords.count());
        return new OffsetMgr(offsetMap);
    }

    public void commit(String topic, int partition, long offset) {
        OffsetBits offsetBits = offsetMap.get(topic + "_" + partition);
        offsetBits.setTrue(offset);
        LOGGER.debug("commit topic {}:{} offset {}", topic, partition, offset);
    }

    public void multiCommit(String topicPartition, List<Long> offsets) {
        OffsetBits offsetBits = offsetMap.get(topicPartition);
        offsetBits.multiSetTrue(offsets);
    }

    public boolean isAllConsumed() {
        boolean result = true;
        for (OffsetBits bits : offsetMap.values()) {
            if (!bits.isPartitionConsumed()) {
                result = false;
                break;
            }
        }
        return result;
    }

    public boolean waitAllConsumed(long timeout, TimeUnit unit) {
        boolean result = true;
        for (OffsetBits bits : offsetMap.values()) {
            if (!bits.waitPartitionConsumed(timeout, unit)) {
                result = false;
                break;
            }
        }
        return result;
    }
}
