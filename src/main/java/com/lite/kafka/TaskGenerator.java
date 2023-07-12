package com.lite.kafka;

import brave.kafka.clients.KafkaTracing;

public abstract class TaskGenerator<K, V> {
    protected KafkaTracing tracing;

    public TaskGenerator() {}

    public TaskGenerator(KafkaTracing tracing) {
        this.tracing = tracing;
    }

    abstract KafkaTask<K, V> generate();

    protected KafkaTask<K, V> decorate() {
        KafkaTask<K, V> task = generate();
        task.kafkaTracing = tracing;
        return task;
    }
}
