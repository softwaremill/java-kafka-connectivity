package com.softwaremill.kafka.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
//@Component
public class BatchedKafkaListenerConsumer {

    @KafkaListener(
            containerFactory = "batchedListenerContainerFactory",
            topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void processMessage(List<Message<String>> content) {
        log.info("Batched KLC Number of messages received: {}", content.size());
        content.forEach(c -> log.info("Batched KLC Record received: value {}", c.getPayload()));
    }

}
