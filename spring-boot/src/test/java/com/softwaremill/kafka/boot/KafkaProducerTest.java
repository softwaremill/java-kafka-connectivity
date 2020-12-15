package com.softwaremill.kafka.boot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerTest {

    @Test
    void test() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("retries", 0);
        props.put("buffer.memory", 33554432);
        props.put("linger.ms", 1);
        props.put("request.timeout.ms", 1000);
        props.put("delivery.timeout.ms", 1002);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        var producer = new KafkaProducer<String, String>(props);

        IntStream.range(0, 30)
                .forEach(i -> {
                    var partition = i % 3;
                    var key = "key" + i;
                    var value = "mess" + i;
                    ProducerRecord<String, String> record = new ProducerRecord<>("some_topic", partition, key, value);
                    System.out.println("sending; partition: " + partition + ", message: " + value);
                    try {
                        producer.send(record).get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });

        assertTrue(true);
        System.out.println("done");
    }

}
