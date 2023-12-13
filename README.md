# lite-kafka-consumer
A lightweight tool for kafka consumer

1. Separates poll thread and consume thread, and supports multiple consume threads
2. Makes a redundant poll to avoid consumer re-balance
3. Supports consumer to reset partition offset
4. Supports multiple threads to consume in sequential order
5. Submit task with a batch of records to multiple consume threads
6. Supports to control the rate of pulling messages by set pollPeriod
7. Supports to control the rate of committing offsets by set commitInterval
8. Integrated with brave trace span
9. Supply a onConsumeError api
10. Wait for more, and any issue is welcome!

```xml
<dependency>
    <groupId>io.github.rushsky518</groupId>
    <artifactId>lite-kafka-consumer</artifactId>
    <version>1.0.1</version>
</dependency>
```

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

KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, new TaskGenerator<String, String>() {
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
}, "biz-poll-thread");

pollThread.start();
```
<br>
<br>
<br>
Summary About KafkaConsumer
（为准确表述，使用中文）

```text
└─KafkaConsumer
    ├─ConsumerCoordinator
    │  ├─ConsumerNetworkClient
    │  ├─Heartbeat
    │  │  └─sentHeartbeat()
    │  ├─HeartbeatThread
    │  ├─maybeAutoCommitOffsetsAsync()
    │  ├─sendFindCoordinatorRequest()
    │  ├─sendHeartbeatRequest()
    │  ├─sendJoinGroupRequest()
    │  └─sendSyncGroupRequest()
    ├─ConsumerNetworkClient
    └─Fetcher
        ├─ConsumerNetworkClient
        ├─fetchedRecords()
        └─sendFetches()
```

对于消费者而言，它必然属于一个消费组，它所订阅的 topic 分区可能分布在不同的 broker 节点上，因此消费者需要解决的问题是：
1. 按照约定的协议加入到 consumer group 中，接收 broker 广播的分区分配方案
2. 向已分配分区（leader）所在的 broker 拉取消息
3. 向对应的 GroupCoordinator 提交 offset 信息
   `Each Kafka server instantiates a coordinator which is responsible for a set of groups. Groups are assigned to coordinators based on their group names.`
4. 考虑到客户端和 broker 之间的网络异常，kafka 引入 re-balance 机制保证异常环境下的高可用，客户端部分实现了按时的心跳，以及某些情况下主动离开消费组


实现上的细节：
1. 启动一个 KafkaConsumer 实例，会创建 2 个线程，1 个线程用来 poll 消息，另 1 个心跳线程用来进行组管理及发送心跳
- KafkaConsumer 在 poll 的过程中，会更新 metadata 信息并记录 poll 时刻，心跳线程综合考虑 poll 时刻和计时器来决定是否发送心跳及 join group 等
2. consumer 的连接管理 
- 遵循按需创建的原则，如果 consumer 需要从该 node 拉取消息，则会创建连接，同时 consumer 与 GroupCoordinator 会另外创建一条连接，隔离消息拉取和 group 管理
   `use MAX_VALUE - node.id as the coordinator id to allow separate connections for the coordinator in the underlying network client layer`
3. 保留了 node = -x 的一条连接
- 这是因为，KafkaConsumer 在启动的时候会 lookupCoordinator，consumer 选择会一个负载较少的节点建立连接，因为此时还不知道 node 的真实 id，所以用了初始的负数标识，这条连接被保存了下来。
