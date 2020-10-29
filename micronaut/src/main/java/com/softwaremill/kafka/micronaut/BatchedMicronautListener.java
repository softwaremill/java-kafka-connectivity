package com.softwaremill.kafka.micronaut;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

@Slf4j
@KafkaListener(groupId = "micronaut-group", clientId = "${kafka.consumers.micronaut-group.client-id}", batch = true)
public class BatchedMicronautListener {

    @Topic("${kafka.consumers.micronaut-group.topic}")
    void receive(List<ConsumerRecord<String, String>> records) {
        log.info("Batch received: {}", records.size());
        records.forEach(rec -> log.info("Message received: key {} value {}", rec.key(), rec.value()));
    }

}