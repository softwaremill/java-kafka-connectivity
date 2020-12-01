package com.softwaremill.kafka.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
class KafkaPartitionedVerticle extends AbstractVerticle {

    private final String topic;
    private final int partition;
    private final Map<String, String> kafkaConfig;

    static KafkaPartitionedVerticle create(String topic, int partition, Map<String, String> kafkaConfig) {
        return new KafkaPartitionedVerticle(topic, partition, kafkaConfig);
    }

    private KafkaPartitionedVerticle(String topic, int partition, Map<String, String> kafkaConfig) {
        this.topic = topic;
        this.partition = partition;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void start() {
        KafkaConsumer.create(vertx, kafkaConfig)
                .assign(new TopicPartition(topic, partition), AsyncResult::result)
                .handler(record -> log.info("Partitioned Kafka consumer. Message read: partition {} key {} value {}", record.partition(), record.key(), record.value()))
                .endHandler(v -> log.info("End of data. Topic: {}, partition: {}", this.topic, this.partition))
                .exceptionHandler(e -> log.error("Partitioned Kafka consumer error", e));
    }

}
