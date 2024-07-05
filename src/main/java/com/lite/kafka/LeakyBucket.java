package com.lite.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 漏桶对消息的消费进行限流，以条数为单位
 */
public class LeakyBucket<T> {
    private int waterMark;
    private int capacity;
    private List<T> elems = new ArrayList<>(capacity);
    // 漏水速率
    private int speed;
    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    public LeakyBucket(int capacity, int speed, TimeUnit timeUnit) {
        this.capacity = capacity;
        this.speed = speed;
        // 提交定时任务
        scheduledExecutor.scheduleAtFixedRate(() -> {
            synchronized (this) {
                waterMark = waterMark - this.speed;
                if (waterMark < 0) {
                    waterMark = 0;
                }
            }
        }, 1, 1, timeUnit);
    }

    public boolean tryAddWater(T elem) {
        synchronized (this) {
            if (elems.size() < capacity) {
                elems.add(elem);
                return true;
            } else {
                return false;
            }
        }
    }

    public void addWater(T elem) throws InterruptedException {
        synchronized (this) {
            elems.add(elem);
            if (elems.size() > capacity) {
                wait();
            }
        }
    }
}
