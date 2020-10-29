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
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("batch.size", 9);
        var producer = new KafkaProducer<String, String>(props);

        IntStream.range(0, 1200)
                .forEach(i -> {
                    var partition = i % 3;
                    ProducerRecord<String, String> record = new ProducerRecord<>("topic3", partition, "key" + i, "mess" + i);
                    System.out.println("sending " + partition + " " + i);
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
