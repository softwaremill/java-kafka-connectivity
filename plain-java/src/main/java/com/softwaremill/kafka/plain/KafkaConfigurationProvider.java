package com.softwaremill.kafka.plain;

import java.io.IOException;
import java.util.Properties;

public class KafkaConfigurationProvider {

    Properties load() throws IOException {
        var config = new Properties();
        try (var reader = this.getClass().getResourceAsStream("/application.properties")) {
            config.load(reader);
        }
        return config;
    }

}
