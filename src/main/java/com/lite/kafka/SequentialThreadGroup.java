package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程按分区顺序消费
 */
public class SequentialThreadGroup implements KafkaWorker {
    private int num = Runtime.getRuntime().availableProcessors();
    private List<ExecutorService> poolGroup;

    public SequentialThreadGroup() {
        init(num);
    }

    public SequentialThreadGroup(int num) {
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
        ConsumerRecord record = kafkaTask.getRecord();
        String key = record.topic() + "_" + record.partition();
        int index = key.hashCode() % this.num;
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
