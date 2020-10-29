package com.softwaremill.kafka.plain;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
class KafkaConsumerTask implements Runnable {

    private final Properties config;

    KafkaConsumerTask(Properties config) {
        this.config = config;
    }

    @Override
    public void run() {
        KafkaConsumerRunner runner = new KafkaConsumerRunner(config, config.getProperty("topic"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                runner.wakeup();
            } catch (Exception exc) {
                log.error("Error while shutting down Kafka consumer", exc);
            } finally {
                log.info("Task stopped :)");
            }
        }));
        runner.run();
    }
}
