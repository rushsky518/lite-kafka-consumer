package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

// VM options: -Dorg.slf4j.simpleLogger.defaultLogLevel=trace
public class TestPause {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pause-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("test"));

        while (true) {
            ConsumerRecords<String, String> firstPoll = consumer.poll(Duration.ofSeconds(1));
            if (firstPoll.isEmpty()) {
                continue;
            }
            Set<TopicPartition> partitions = firstPoll.partitions();
            consumer.pause(partitions);
            break;
        }

        while (true) {
            ConsumerRecords<String, String> secondPoll = consumer.poll(Duration.ofSeconds(1));
            if (secondPoll.isEmpty()) {
                continue;
            }
            ConsumerRecord<String, String> record = secondPoll.iterator().next();
            System.out.println(record);
        }
    }
}
