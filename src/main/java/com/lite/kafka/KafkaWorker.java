package com.lite.kafka;

public interface KafkaWorker {
    void submit(KafkaTask kafkaTask);
    void shutdown();
}
