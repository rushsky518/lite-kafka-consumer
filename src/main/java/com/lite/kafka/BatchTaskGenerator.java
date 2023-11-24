package com.lite.kafka;

/**
 * generate task
 * @param <K> record key
 * @param <V> record value
 */
public abstract class BatchTaskGenerator<K, V> {
    public BatchTaskGenerator() {}

    /**
     * implemented by caller
     * @return a KafkaTask
     */
    public abstract BatchKafkaTask<K, V> generate();

    BatchKafkaTask<K, V> decorate() {
        BatchKafkaTask<K, V> task = generate();
        return task;
    }
}
