package com.lite.kafka;

import brave.kafka.clients.KafkaTracing;

/**
 * generate task
 * @param <K> record key
 * @param <V> record value
 */
public abstract class TaskGenerator<K, V> {
    protected KafkaTracing tracing;

    public TaskGenerator() {}

    public TaskGenerator(KafkaTracing tracing) {
        this.tracing = tracing;
    }

    /**
     * implemented by caller
     * @return a KafkaTask
     */
    public abstract KafkaTask<K, V> generate();

    KafkaTask<K, V> decorate() {
        KafkaTask<K, V> task = generate();
        if (tracing != null) {
            task.kafkaTracing = tracing;
        }
        return task;
    }
}
