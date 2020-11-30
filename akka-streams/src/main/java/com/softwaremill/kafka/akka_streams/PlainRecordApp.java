package com.softwaremill.kafka.akka_streams;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

public class PlainRecordApp {

    public static void main(String[] args) {
        var system = ActorSystem.create("PlainRecordApp");
        var materializer = ActorMaterializer.create(system);
        var configuration = system.settings().config();

        var consumerConfig = configuration.getConfig("plain-kafka-consumer");
        var consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer());

        var topicName = consumerConfig.getString("topic");
        String result = Consumer
                .plainSource(consumerSettings, Subscriptions.topics(topicName))
                .idleTimeout(Duration.ofSeconds(5))
                .map(consumerRecord -> {
                    System.out.printf("%s Message key %s, value %s, partition %d%n",
                            Thread.currentThread().getName(), consumerRecord.key(),
                            consumerRecord.value(), consumerRecord.partition());
                    return consumerRecord;
                })
                .runWith(Sink.ignore(), materializer)
                .toCompletableFuture()
                .handle(AppSupport.doneHandler())
                .join();

        System.out.println("PlainRecordApp done with " + result);

        materializer.shutdown();
        system.terminate();
    }

}
