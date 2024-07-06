package com.lite.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LeakyBucketTest {
    public static void main(String[] args) throws InterruptedException {
        // 10 req/s
        final LeakyBucket leakyBucket = new LeakyBucket(1, 500, TimeUnit.MILLISECONDS);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (;true;) {
            TimeUnit.MILLISECONDS.sleep(100);
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
