package com.xuefei.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description: 生产者拦截器示例
 * @author: xuefei
 * @date: 2023/06/11 23:07
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

	private volatile long sendSuccess = 0;
	private volatile long sendFailure = 0;

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		String modifiedValue = "prefix1-" + record.value();
		return new ProducerRecord<>(record.topic(),
				record.partition(), record.timestamp(),
				record.key(), modifiedValue, record.headers());
	}

	@Override
	public void onAcknowledgement(
			RecordMetadata recordMetadata,
			Exception e) {
		if (e == null) {
			sendSuccess++;
		} else {
			sendFailure++;
		}
	}

	@Override
	public void close() {
		double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
		System.out.println("[INFO] 发送成功率="
				+ String.format("%f", successRatio * 100) + "%");
	}

	@Override
	public void configure(Map<String, ?> map) {
	}

}

