package com.lite.kafka;

public interface TaskGenerator<K, V> {
    KafkaTask<K, V> generate();
}
