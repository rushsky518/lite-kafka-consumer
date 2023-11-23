package com.lite.kafka;

import brave.Span;
import brave.Tracer;
import brave.kafka.clients.KafkaTracing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * 消息的简单封装
 */
public abstract class KafkaTask<K ,V> implements Runnable, Consumer<ConsumerRecord<K ,V>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTask.class);
    // 重试次数
    private static int RETRY_LIMIT = 1;

    protected ConsumerRecord<K, V> record;
    protected OffsetMgr offsetMgr;

    protected KafkaTracing kafkaTracing;

    public KafkaTask() {}

    public KafkaTask(ConsumerRecord<K, V> record, OffsetMgr offsetMgr) {
        this.record = record;
        this.offsetMgr = offsetMgr;
    }

    @Override
    public void run() {
        Span span = null;
        if (kafkaTracing != null) {
            // Grab any span from the record. The topic and key are automatically tagged
            span = kafkaTracing.nextSpan(record).name(this.getClass().getName()).start();
            Tracer tracer = kafkaTracing.messagingTracing().tracing().tracer();
            Tracer.SpanInScope ws = tracer.withSpanInScope(span);
        }
        int retry = 0;
        boolean committed = false;
        while (retry < RETRY_LIMIT) {
            try {
                accept(record);
                commit();
                committed = true;
                break;
            } catch (Exception ex) {
                if (span != null) {
                    span.error(ex);
                }
                LOGGER.error("error", ex);
                retry += 1;
            }
        }
        if (!committed) {
            commit();
        }
        if (span != null) {
            span.finish();
        }
    }

    private void commit() {
        offsetMgr.commit(record.topic(), record.partition(), record.offset());
    }

    protected ConsumerRecord<K, V> getRecord() {
        return record;
    }

}
