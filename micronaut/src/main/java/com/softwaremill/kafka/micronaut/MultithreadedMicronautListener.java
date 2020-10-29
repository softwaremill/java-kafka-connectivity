package com.softwaremill.kafka.micronaut;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@KafkaListener(groupId = "micronaut-group", clientId = "${kafka.consumers.micronaut-group.client-id}", threads = 5)
public class MultithreadedMicronautListener {

    // https://blog.wick.technology/configuring-micronaut-kakfa-serdes/
    // https://micronaut-projects.github.io/micronaut-kafka/latest/guide/#kafkaListener
    // https://github.com/micronaut-projects/micronaut-kafka
    // https://piotrminkowski.com/2019/08/06/kafka-in-microservices-with-micronaut/

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