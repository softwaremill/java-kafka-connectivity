package com.softwaremill.kafka.boot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

@Slf4j
public class CustomMessageListener implements MessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        log.info("CML Record received. Key: {}, value: {}, partition: {}", data.key(), data.value(), data.partition());
    }
}
