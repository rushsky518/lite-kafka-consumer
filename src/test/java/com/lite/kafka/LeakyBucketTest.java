package com.lite.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LeakyBucketTest {
    public static void main(String[] args) throws InterruptedException {
        // 10 req/s
        final LeakyBucket leakyBucket = new LeakyBucket(1, 1, TimeUnit.SECONDS);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ExecutorService pool = Executors.newFixedThreadPool(64);
        for (;true;) {
            TimeUnit.MILLISECONDS.sleep(500);
            pool.submit(() -> {
                try {
                    leakyBucket.addWater(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // do a dummy job, print the time
                System.out.println(sdf.format(new Date()));
            });

        }
    }
}
