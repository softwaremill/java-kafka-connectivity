package com.softwaremill.kafka.micronaut;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@KafkaListener(groupId = "micronaut-group", clientId = "${kafka.consumers.micronaut-group.client-id}", threads = 5)
public class MultithreadedMicronautListener {

//    @Topic("${kafka.consumers.micronaut-group.topic}")
    void receive(@KafkaKey String key, String value, int partition) {
        switch (partition) {
            case 0:
                log.info("Message from partition 0: key {} value {}", key, value);
                break;
            case 1:
                log.info("Message from partition 1: key {} value {}", key, value);
                break;
            case 2:
                log.info("Message from partition 2: key {} value {}", key, value);
                break;
            default:
                log.error("Message (key {}, value {}) from unexpected partition ({}) received.", key, value, partition);
        }
    }
}