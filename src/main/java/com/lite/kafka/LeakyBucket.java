package com.lite.kafka;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简易的漏桶算法，用于消息消费的限流
 */
public class LeakyBucket {
    private final AtomicInteger waterMark;
    // 漏桶的容量，对应业务可以承受的突发流量
    private final int capacity;
    // 漏桶的出水速率
    private final int speed;
    private final ScheduledExecutorService scheduledExecutor;

    public LeakyBucket(int speed, int period, TimeUnit timeUnit) {
        this(100, speed, period, timeUnit, null);
    }

    public LeakyBucket(int speed, int period, TimeUnit timeUnit, ScheduledExecutorService executor) {
        this(100, speed, period, timeUnit, executor);
    }

    public LeakyBucket(int capacity, int speed, int period, TimeUnit timeUnit, ScheduledExecutorService executor) {
        if (capacity <= 0 || speed <= 0) {
            throw new IllegalArgumentException("Capacity and speed must be positive.");
        }
        this.capacity = capacity;
        this.speed = speed;
        this.waterMark = new AtomicInteger(capacity);
        if (executor == null) {
            this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("LeakyBucket-Scheduler");
                return t;
            });
        } else {
            this.scheduledExecutor = executor;
        }

        this.scheduledExecutor.scheduleAtFixedRate(this::leak, 0, period, timeUnit);
    }

    private void leak() {
        try {
            int newWaterMark = waterMark.addAndGet(-speed);
            if (newWaterMark < 0) {
                waterMark.set(0);
            }
            synchronized (this) {
                notifyAll();
            }
        } catch (Exception e) {
            // Log the exception and continue
            System.err.println("Error in leak task: " + e.getMessage());
        }
    }

    public boolean tryAddWater(int amount) {
        int curWaterMark = waterMark.get();
        final int newWaterMark = curWaterMark + amount;
        if (newWaterMark > capacity) {
            return false;
        }
        int updated = waterMark.addAndGet(amount);
        return updated == newWaterMark;
    }

    public void addWater(int amount) throws InterruptedException {
        while (true) {
            int curWaterMark = waterMark.get();
            int newWaterMark = curWaterMark + amount;
            if (newWaterMark <= capacity) {
                int updated = waterMark.addAndGet(amount);
                if (updated == newWaterMark) {
                    break;
                }
            } else {
                synchronized (this) {
                    wait();
                }
            }
        }
    }

    public void shutdown() {
        scheduledExecutor.shutdown();
    }
}