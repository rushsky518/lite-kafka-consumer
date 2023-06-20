package com.lite.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 对应一个分区
 */
public class OffsetBits {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetBits.class);

    private long base;
    private int size;
    private final BitSet bits;
    private final CountDownLatch cdLatch;

    public OffsetBits(long base, int size) {
        this.base = base;
        this.size = size;
        this.bits = new BitSet(size);
        this.cdLatch = new CountDownLatch(size);
    }

    public void setTrue(long offset) {
        long index = offset - base;
        bits.set((int) index, true);
        this.cdLatch.countDown();
    }

    public boolean isPartitionConsumed() {
        return bits.cardinality() == size;
    }

    public boolean waitPartitionConsumed(long timeout, TimeUnit unit) {
        if (bits.cardinality() < size) {
            try {
                LOGGER.debug("wait {} msg(s) consume completed", size);
                cdLatch.await(timeout, unit);
            } catch (InterruptedException e) {
                LOGGER.error("interrupted", e);
            }
        }
        LOGGER.debug("resume");
        return bits.cardinality() == size;
    }
}