package com.lite.kafka;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LeakyBucket {
    private final AtomicInteger waterMark;
    private final int capacity;
    private final int speed;
    private final ScheduledExecutorService scheduledExecutor;

    public LeakyBucket(int speed, TimeUnit timeUnit) {
        this(100, speed, timeUnit);
    }

    public LeakyBucket(int capacity, int speed, TimeUnit timeUnit) {
        if (capacity <= 0 || speed <= 0) {
            throw new IllegalArgumentException("Capacity and speed must be positive.");
        }
        this.capacity = capacity;
        this.speed = speed;
        this.waterMark = new AtomicInteger(capacity);
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("LeakyBucket-Scheduler");
            return t;
        });
        scheduledExecutor.scheduleAtFixedRate(this::leak, 0, timeUnit.toMillis(1), TimeUnit.MILLISECONDS);
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
        int currentWaterMark = waterMark.get();
        if (currentWaterMark + amount > capacity) {
            return false;
        }
        waterMark.addAndGet(amount);
        return true;
    }

    public void addWater(int amount) throws InterruptedException {
        while (true) {
            int currentWaterMark = waterMark.get();
            if (currentWaterMark + amount <= capacity) {
                waterMark.addAndGet(amount);
                break;
            }
            synchronized (this) {
                wait();
            }
        }
    }

    public void shutdown() {
        scheduledExecutor.shutdown();
    }
}