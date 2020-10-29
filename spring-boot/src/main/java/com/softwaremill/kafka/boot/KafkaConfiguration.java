package com.softwaremill.kafka.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

@Configuration
public class KafkaConfiguration {

    private final KafkaProperties properties;
    private final Environment environment;

    @Autowired
    KafkaConfiguration(KafkaProperties properties, Environment environment) {
        this.properties = properties;
        this.environment = environment;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> batchedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> multiThreadedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        return factory;
    }

//    @Bean
    MessageListenerContainer container() {
        String topic = environment.getProperty("spring.kafka.consumer.topic");
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(new CustomMessageListener());
        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);
    }

//    @Bean
    MessageListenerContainer concurrentContainer() {
        String topic = environment.getProperty("spring.kafka.consumer.topic");
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(new CustomMessageListener());
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory(), containerProperties);
        container.setConcurrency(3);
        return container;
    }

    private ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties());
    }

}
