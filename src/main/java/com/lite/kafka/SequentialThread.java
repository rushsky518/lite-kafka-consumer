package com.lite.kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单线程顺序消费
 */
public class SequentialThread implements KafkaWorker {
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SequentialThread() {}

    @Override
    public void submit(KafkaTask kafkaTask) {
        executorService.submit(kafkaTask);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
