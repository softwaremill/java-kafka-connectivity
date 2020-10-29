package com.softwaremill.kafka.micronaut;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@KafkaListener(groupId = "micronaut-group", clientId = "${kafka.consumers.micronaut-group.client-id}")
public class MicronautListener {

//    @Topic("${kafka.consumers.micronaut-group.topic}")
    void receive(@KafkaKey String key, String value) {
        log.info("Message received: key {} value {}", key, value);
    }

}