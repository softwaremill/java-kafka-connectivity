package com.softwaremill.kafka.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
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
                .subscribe(topic, subscriptionResultHandler())
                .handler(record -> log.info("Single Kafka consumer. Message read: partition {} key {} value {}", record.partition(), record.key(), record.value()))
                .endHandler(v -> log.info("End of data. Topic: {}", this.topic))
                .exceptionHandler(e -> log.error("Single Kafka consumer error", e));
    }

    private Handler<AsyncResult<Void>> subscriptionResultHandler() {
        return result -> {
            if (result.succeeded()) {
                log.info("Subscription to {} succeeded", topic);
            } else {
                log.error("Something went wrong when subscribing to {}", topic, result.cause());
            }
        };
    }

}
