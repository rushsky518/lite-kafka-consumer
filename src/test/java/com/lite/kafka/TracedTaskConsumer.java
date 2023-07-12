package com.lite.kafka;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.messaging.MessagingRuleSampler;
import brave.messaging.MessagingTracing;
import brave.sampler.Sampler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static brave.messaging.MessagingRequestMatchers.operationEquals;

public class TracedTaskConsumer {

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

        Tracing tracing = tracingBuilder(Sampler.ALWAYS_SAMPLE).build();
        KafkaTracing kafkaTracing = KafkaTracing.create(tracing);

        KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, new TaskGenerator<String, String>(kafkaTracing) {
            @Override
            KafkaTask<String, String> generate() {
                return new KafkaTask<String, String>() {
                    @Override
                    public void accept(ConsumerRecord<String, String> record) {
                        System.out.printf("thread:%s offset=%d, key=%s, value=%s\n", Thread.currentThread(),
                                this.record.offset(), this.record.key(), this.record.value());
                    }
                };
            }
        }, "biz-poll-thread");

        pollThread.start();
    }

    protected static Tracing.Builder tracingBuilder(Sampler sampler) {
        return Tracing.newBuilder()
                .localServiceName("XX")
                .localIp("127.0.0.1") // Prevent implicit lookups
                .sampler(sampler);
    }
}
