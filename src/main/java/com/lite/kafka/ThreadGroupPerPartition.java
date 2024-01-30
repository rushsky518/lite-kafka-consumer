package com.lite.kafka;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Specify a thread to consume one partition
 */
public class ThreadGroupPerPartition implements KafkaWorker {
    // capacity number of threads
    private int limits;

    public ThreadGroupPerPartition() {
        this(128);
    }

    public ThreadGroupPerPartition(int limits) {
        this.limits = limits;
    }

    private final Map<String, ExecutorService> executors = new TreeMap<>();

    @Override
    public void submit(KafkaTask task) {
        String tp = task.record.topic() + "_" + task.record.partition();

        ExecutorService executorService = executors.get(tp);
        if (executorService == null) {
            if (executors.size() < limits) {
                executorService = Executors.newSingleThreadExecutor();
                executors.put(tp, executorService);
            } else {
                throw new RuntimeException("the number of threads exceeds " + limits);
            }
        }
        executorService.submit(task);
    }

    @Override
    public void shutdown() {
        for (Map.Entry<String, ExecutorService> entry : executors.entrySet()) {
            ExecutorService executorService = entry.getValue();
            executorService.shutdown();
        }
    }

    @Override
    public void refresh() {
        shutdown();
        executors.clear();
    }
}
