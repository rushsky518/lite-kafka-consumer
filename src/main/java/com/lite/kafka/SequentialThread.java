package com.lite.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 支持分区按顺序消费
 */
public class SequentialThread implements KafkaWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequentialThread.class);
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SequentialThread() {}

    @Override
    public void submit(KafkaTask kafkaTask) {
        executorService.submit(kafkaTask);
    }

    @Override
    public void submit(BatchKafkaTask kafkaTask) {
        executorService.submit(kafkaTask);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("", e);
        }
    }
}
