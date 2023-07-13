package com.lite.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程消费消息
 */
public class MultiThreadGroup implements KafkaWorker {
    private int num = Runtime.getRuntime().availableProcessors();
    private List<ExecutorService> poolGroup;
    private int tasks;

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
    public void shutdown() {
        for (ExecutorService executorService : poolGroup) {
            executorService.shutdown();
        }
    }
}
