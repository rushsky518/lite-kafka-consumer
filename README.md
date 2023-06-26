# lite-kafka-consumer
a lightweight tool for kafka consumer

1. separates poll thread and consume thread, and supports multi consume thread
2. makes a redundant poll to avoid consumer re-balance
3. supports consumer to reset partition offset
4. wait for more ...

```java
// demo code, separate kafka poll thread and a single consume thread
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

KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, () -> new KafkaTask<String, String>() {
    @Override
    public void accept(ConsumerRecord<String, String> record) {
        System.out.printf("offset=%d, key=%s, value=%s\n", record.offset(), record.key(), record.value());
    }
}, "biz-poll-thread");

pollThread.start();
```
