package com.lite.kafka;

public interface KafkaWorker {
    void submit(KafkaTask kafkaTask);
    default void submit(BatchKafkaTask kafkaBatchTask) {}
    void shutdown();
}
