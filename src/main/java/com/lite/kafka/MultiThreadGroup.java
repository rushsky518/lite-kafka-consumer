package com.lite.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 多线程消费消息
 */
public class MultiThreadGroup implements KafkaWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadGroup.class);
    private int num = Runtime.getRuntime().availableProcessors();
    private List<ExecutorService> poolGroup;
    private volatile int tasks;

    public MultiThreadGroup() {
        init(num);
    }

    public MultiThreadGroup(int num) {
        this.num = num;
        init(num);
    }

    private void init(int num) {
        if (num > 0) {
            this.num = num;
        }

        poolGroup = new ArrayList<>(num);
        for (int i = 0; i < num; i ++) {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            poolGroup.add(executorService);
        }
    }

    @Override
    public void submit(KafkaTask kafkaTask) {
        tasks ++;
        int index = tasks % this.num;
        ExecutorService executorService = poolGroup.get(index);
        executorService.submit(kafkaTask);
    }


    @Override
    public void submit(BatchKafkaTask kafkaBatchTask) {
        tasks ++;
        int index = tasks % this.num;
        ExecutorService executorService = poolGroup.get(index);
        executorService.submit(kafkaBatchTask);
    }
    
    @Override
    public void shutdown() {
        for (ExecutorService executorService : poolGroup) {
            executorService.shutdown();
        }
        for (ExecutorService executorService : poolGroup) {
            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("", e);
            }
        }
    }
}
