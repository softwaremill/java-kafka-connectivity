package com.softwaremill.kafka.plain;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executors;

@Slf4j
public class KafkaConsumerApp {

    public static void main(String[] args) throws IOException {
        log.info("Plain Kafka application started");

        var config = new KafkaConfigurationProvider().load();
        var executor = Executors.newSingleThreadExecutor();

        executor.execute(new KafkaConsumerTask(config));

        var running = true;
        var scanner = new Scanner(System.in);
        while(running) {
            var line = scanner.nextLine();
            if ("stop".equals(line)) {
                running = false;
                scanner.close();
            }
        }

        log.info("Processing finished!");
    }

}
