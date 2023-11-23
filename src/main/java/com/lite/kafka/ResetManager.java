package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/***
 * offset reset manager
 */
public class ResetManager {
    private volatile boolean stop = false;
    public static final String RESET_TOPIC = "RESET_OFFSET";
    private List<KafkaPollThread> pollThreads = new ArrayList<>();

    public static ResetManager getResetManager(String bootstrapServers, String serverIp, int port) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        String ipPort = String.format("%s:%d", serverIp, port);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ipPort);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new ResetManager(props);
    }

    public ResetManager(Properties properties) {
        new Thread(() -> {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
            TopicPartition tp = new TopicPartition(RESET_TOPIC, 0);
            kafkaConsumer.assign(Collections.singleton(tp));
            while (!stop) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    ResetOps resetOps = ResetOps.fromString(value);
                    for (KafkaPollThread pollThread : pollThreads) {
                        if (pollThread.groupId().equals(resetOps.getGroupId())) {
                            pollThread.resetOps = resetOps;
                            break;
                        }
                    }
                } // iterate record
                kafkaConsumer.commitSync();
            }

            kafkaConsumer.unsubscribe();
        }, this.getClass().getSimpleName()).start();

    }

    public void register(KafkaPollThread pollThread) {
        pollThreads.add(pollThread);
    }

    public void stop() {
        this.stop = true;
    }
}