package com.softwaremill.kafka.plain;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
class KafkaConsumerRunner {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    KafkaConsumerRunner(Properties config, String topic) {
        this.consumer = new KafkaConsumer<>(config);
        this.topic = topic;
    }

    void run() {
        log.info("Kafka runner started");

        try {
            consumer.subscribe(Collections.singleton(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(rec -> log.info("Record received. Key: {}, value: {}", rec.key(), rec.value()));
            }
        }
        catch (WakeupException exc) {
            log.error("Error on consumer wakeup call", exc);
        } finally {
            consumer.close();
            log.info("Consumer finally closed");
        }
    }

    void wakeup() {
        consumer.wakeup();
    }
}
