package com.xuefei.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @description: 生产者客户端
 * @author: xuefei
 * @date: 2023/06/11 19:03
 */
public class KafkaProducerAnalysis {

	public static final String brokerList = "localhost:9092";
	public static final String topic = "topic-demo";

	public static Properties initConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// 重试次数，默认为0
		props.put(ProducerConfig.RETRIES_CONFIG, 10);

		props.put("client.id", "producer.client.id.demo");
		return props;
	}

	public static void main(String[] args) {

		Properties props = initConfig();
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello, Kafka!");

//		try {
//			// 异步
//			producer.send(record);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

//		try {
//			Future<RecordMetadata> future = producer.send(record);
//			// RecordMetadata 对象里包含了消息的一些元数据信息，比如当前消息的主题、分区号、分区中的偏移量（offset）、时间戳等
//			RecordMetadata metadata = future.get();
//			System.out.println(metadata.topic() + "-" +
//					metadata.partition() + ":" + metadata.offset());
//		} catch (ExecutionException | InterruptedException e) {
//			e.printStackTrace();
//		}

		// send() 方法的返回值类型就是 Future，而 Future 本身就可以用作异步的逻辑处理。这样做不是不行，
		// 只不过 Future 里的 get() 方法在何时调用，以及怎么调用都是需要面对的问题，消息不停地发送，
		// 那么诸多消息对应的 Future 对象的处理难免会引起代码处理逻辑的混乱。
		// 使用 Callback 的方式非常简洁明了，Kafka 有响应时就会回调，要么发送成功，要么抛出异常。
//		producer.send(record, new Callback() {
//
//			@Override
//			public void onCompletion(RecordMetadata metadata, Exception exception) {
//				if (exception != null) {
//					exception.printStackTrace();
//				} else {
//					System.out.println(metadata.topic() + "-" +
//							metadata.partition() + ":" + metadata.offset());
//				}
//			}
//
//		});

		int i = 0;
		while (i < 100) {
			record = new ProducerRecord<>(topic, "msg" + i++);
			try {
				producer.send(record).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		// 需要调用 KafkaProducer 的 close() 方法来回收资源
		producer.close();

	}

}
