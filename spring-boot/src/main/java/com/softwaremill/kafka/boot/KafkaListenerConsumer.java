package com.softwaremill.kafka.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaListenerConsumer {

    @KafkaListener(
//            concurrency = "3",
//            containerFactory = "multiThreadedListenerContainerFactory",
            topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void processMessage(Message<String> content) {
        log.info(Thread.currentThread().getName() + " KLC Record received: value {}", content);
    }

}
