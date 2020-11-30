package com.softwaremill.kafka.akka_streams;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

public class PartitionedApp {

    public static void main(String[] args) {
        var system = ActorSystem.create("PartitionedApp");
        var materializer = ActorMaterializer.create(system);

        var configuration = system.settings().config();

        var maxPartitions = 5;

        var consumerConfig = configuration.getConfig("partitioned-kafka-consumer");
        var consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer());

        String topicName = consumerConfig.getString("topic");
        String result = Consumer
                .plainPartitionedSource(consumerSettings, Subscriptions.topics(topicName))
                .idleTimeout(Duration.ofSeconds(5))
                .mapAsync(maxPartitions, pair -> {
                    Source<ConsumerRecord<String, String>, NotUsed> source = pair.second();
                    return source
                            .map(record -> {
                                System.out.printf("%s Message key %s, value %s, partition %d%n",
                                        Thread.currentThread().getName(), record.key(),
                                        record.value(), record.partition());
                                return record;
                            })
                            .runWith(Sink.ignore(), materializer);
                })
                .runWith(Sink.ignore(), materializer)
                .toCompletableFuture()
                .handle(AppSupport.doneHandler())
                .join();

        System.out.println("PartitionedApp done with " + result);

        materializer.shutdown();
        system.terminate();
    }

}
