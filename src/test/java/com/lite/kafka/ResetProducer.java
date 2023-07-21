package com.lite.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.lite.kafka.ResetConsumer.RESET_GROUP_ID;
import static com.lite.kafka.ResetManager.RESET_TOPIC;

public class ResetProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ResetOps resetOps = new ResetOps(RESET_GROUP_ID, "test", 0, 10);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(RESET_TOPIC, resetOps.toString()));
        System.out.printf("send %s\n", resetOps);

        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
