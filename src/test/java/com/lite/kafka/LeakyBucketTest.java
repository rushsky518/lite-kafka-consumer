package com.lite.kafka;

import java.util.concurrent.TimeUnit;

public class LeakyBucketTest {
    public static void main(String[] args) throws InterruptedException {
        LeakyBucket leakyBucket = new LeakyBucket(10, 1, TimeUnit.SECONDS);
        for (;true;) {
            leakyBucket.addWater(2);
            System.out.println(System.currentTimeMillis());
        }
    }
}
