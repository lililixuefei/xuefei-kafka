package com.xuefei.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: 消费者客户端示例
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

	public static Properties initConfig(){
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

		try {
			while (isRunning.get()) {
				ConsumerRecords<String, String> records =
						consumer.poll(Duration.ofMillis(1000).toMillis());
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("topic = " + record.topic()
							+ ", partition = " + record.partition()
							+ ", offset = " + record.offset());
					System.out.println("key = " + record.key()
							+ ", value = " + record.value());
					//do something to process record.
				}
			}
		} catch (Exception e) {
			log.error("occur exception ", e);
		} finally {
			consumer.close();
		}
	}
}
