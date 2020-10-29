package com.softwaremill.kafka.micronaut;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@KafkaListener(groupId = "micronaut-group", clientId = "${kafka.consumers.micronaut-group.client-id}")
public class SingleMicronautListener {

//    @Topic("${kafka.consumers.micronaut-group.topic}")
    void receive(@KafkaKey String key, String value) {
        log.info("Message received: key {} value {}", key, value);
    }

}