package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface TaskHandler<K, V> {
    KafkaTask<K, V> accept(ConsumerRecord<K, V> record, OffsetMgr offsetMgr);
}
