package com.xuefei.kafka.serialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @description:
 * @author: xuefei
 * @date: 2023/06/11 22:55
 */
public class CompanySerializerTest {


	public static final String brokerList = "localhost:9092";
	public static final String topic = "topic-demo";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		//代码清单4-3 自定义序列化器使用示例
		Properties properties = new Properties();
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				CompanySerializer.class.getName());
		properties.put("bootstrap.servers", brokerList);

		KafkaProducer<String, Company> producer =
				new KafkaProducer<>(properties);
		Company company = Company.builder().name("hiddenkafka")
				.address("China").build();
		ProducerRecord<String, Company> record =
				new ProducerRecord<>(topic, company);
		producer.send(record).get();
	}


}
