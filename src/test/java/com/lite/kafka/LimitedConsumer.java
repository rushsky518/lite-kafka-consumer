package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LimitedConsumer {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "your-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("test"));


        TaskGenerator<String, String> taskGenerator = new TaskGenerator<String, String>() {
            @Override
            public KafkaTask<String, String> generate() {
                return new KafkaTask<String, String>() {
                    @Override
                    public void accept(ConsumerRecord<String, String> record) {
                        System.out.printf("thread:%s offset=%d, key=%s, value=%s\n", Thread.currentThread(),
                                this.record.offset(), this.record.key(), this.record.value());
                    }
                };
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        // 2 req/s
        LeakyBucket leakyBucket = new LeakyBucket(2, 1, TimeUnit.SECONDS, executor);
        taskGenerator.setLeakyBucket(leakyBucket);

        KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, taskGenerator, "biz-poll-thread", new MultiThreadGroup(4));

        pollThread.start();
    }
}
