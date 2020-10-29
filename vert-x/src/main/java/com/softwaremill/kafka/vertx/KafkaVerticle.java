package com.softwaremill.kafka.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
class KafkaVerticle extends AbstractVerticle {

    private final String topic;
    private final Map<String, String> kafkaConfig;

    static KafkaVerticle create(String topic, Map<String, String> kafkaConfig) {
        return new KafkaVerticle(topic, kafkaConfig);
    }

    private KafkaVerticle(String topic, Map<String, String> kafkaConfig) {
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void start() {
        KafkaConsumer.create(vertx, kafkaConfig)
                .subscribe(topic)
                .handler(record -> log.info("Vertx Kafka consumer. Message read: partition {} key {} value {}", record.partition(), record.key(), record.value()))
                .exceptionHandler(e -> log.error("Vertx Kafka consumer error", e));
    }

}
