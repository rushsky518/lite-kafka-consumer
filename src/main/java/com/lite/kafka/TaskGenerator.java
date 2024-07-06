package com.lite.kafka;

import brave.kafka.clients.KafkaTracing;

/**
 * generate task
 * @param <K> record key
 * @param <V> record value
 */
public abstract class TaskGenerator<K, V> {
    protected KafkaTracing tracing;
    protected LeakyBucket leakyBucket;

    public TaskGenerator() {}

    public TaskGenerator(KafkaTracing tracing) {
        this.tracing = tracing;
    }

    public void setLeakyBucket(LeakyBucket leakyBucket) {
        this.leakyBucket = leakyBucket;
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
        if (leakyBucket != null) {
            task.leakyBucket = leakyBucket;
        }
        return task;
    }

    protected void shutdown() {
        if (this.leakyBucket != null) {
            this.leakyBucket.shutdown();
        }
    }
}
