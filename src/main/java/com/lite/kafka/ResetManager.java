package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/***
 * offset 重置管理
 */
public class ResetManager {
    private volatile boolean stop = false;
    private String RESET_TOPIC = "RESET_OFFSET";
    private List<KafkaPollThread> pollThreads = new ArrayList<>();

    public ResetManager(Properties properties) {
        new Thread(() -> {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(RESET_TOPIC));
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
        }, this.getClass().getSimpleName()).start();

    }

    public void register(KafkaPollThread pollThread) {
        pollThreads.add(pollThread);
    }

    public void stop() {
        this.stop = true;
    }
}
