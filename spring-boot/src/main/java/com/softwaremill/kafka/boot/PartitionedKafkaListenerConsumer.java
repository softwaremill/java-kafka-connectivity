package com.softwaremill.kafka.boot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
public class PartitionedKafkaListenerConsumer {

    @KafkaListener(
            clientIdPrefix = "part0",
            topics = "${spring.kafka.consumer.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            topicPartitions = {
                    @TopicPartition(topic = "${spring.kafka.consumer.topic}", partitions = {"0"})
            })
    public void partition0(ConsumerRecord<String, String> content) {
        log.info("PKLC Record received: partition 0, key {}, value {}", content.key(), content.value());
    }

    @KafkaListener(
            clientIdPrefix = "part1",
            topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}",
            topicPartitions = {
                    @TopicPartition(topic = "${spring.kafka.consumer.topic}", partitions = {"1"})
            })
    public void partition1(ConsumerRecord<String, String> content) {
        log.info("PKLC Record received: partition 1, key {}, value {}", content.key(), content.value());
    }

    @KafkaListener(
            clientIdPrefix = "part2",
            topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}",
            topicPartitions = {
                    @TopicPartition(topic = "${spring.kafka.consumer.topic}", partitions = {"2"})
            })
    public void partition2(ConsumerRecord<String, String> content) {
        log.info("PKLC Record received: partition 2, key {}, value {}", content.key(), content.value());
    }

}
