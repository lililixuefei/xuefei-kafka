package com.xuefei.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: xuefei
 * @date: 2022/04/16 16:42
 */
@RestController
public class KafkaSyncProducerController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 发送消息
     *
     * @param normalMessage
     */
    @GetMapping("/kafka/sync/{message}")
    public void sendMessage1(@PathVariable("message") String normalMessage) {
        System.out.println(normalMessage);
        kafkaTemplate.send("topic1", normalMessage);
    }
}
