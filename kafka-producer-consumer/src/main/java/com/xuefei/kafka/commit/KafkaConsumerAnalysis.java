package com.xuefei.kafka.commit;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: 位移提交
 * @author: xuefei
 * @date: 2023/06/12 22:40
 */
@Slf4j
public class KafkaConsumerAnalysis {

	public static final String brokerList = "localhost:9092";
	public static final String topic = "topic-demo";
	public static final String groupId = "group.demo";
	public static final AtomicBoolean isRunning = new AtomicBoolean(true);

//	public static Properties initConfig() {
//		Properties props = new Properties();
//		props.put("key.deserializer",
//				"org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("value.deserializer",
//				"org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("bootstrap.servers", brokerList);
//		props.put("group.id", groupId);
//		props.put("client.id", "consumer.client.id.demo");
//		return props;
//	}

	public static Properties initConfig() {
		Properties props = new Properties();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client.id.demo");
		return props;
	}

	public static void main(String[] args) {
		Properties props = initConfig();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));

		//代码清单11-1 消费位移的演示
		TopicPartition tp = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(tp));
		long lastConsumedOffset = -1;//当前消费到的位移
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			if (records.isEmpty()) {
				break;
			}
			List<ConsumerRecord<String, String>> partitionRecords
					= records.records(tp);
			lastConsumedOffset = partitionRecords
					.get(partitionRecords.size() - 1).offset();
			consumer.commitSync();//同步提交消费位移
		}
		System.out.println("comsumed offset is " + lastConsumedOffset);
		OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
		System.out.println("commited offset is " + offsetAndMetadata.offset());
		long posititon = consumer.position(tp);
		System.out.println("the offset of the next record is " + posititon);
	}
}
