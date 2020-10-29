package com.softwaremill.kafka.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class KafkaListenerConsumer {

    @KafkaListener(
//            concurrency = "2",
//            containerFactory = "multiThreadedListenerContainerFactory",
            topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void processMessage(List<Message<String>> content) {
        content.forEach(c -> log.info("KLC Record received: value {}", c.getPayload()));
    }

}
