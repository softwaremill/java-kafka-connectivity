package com.softwaremill.kafka.akka_streams;

import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class CommittableApp {

    public static void main(String[] args) {
        var system = ActorSystem.create("CommittableApp");
        var materializer = ActorMaterializer.create(system);
        var configuration = system.settings().config();

        var consumerConfig = configuration.getConfig("committable-kafka-consumer");
        var committerSettings = consumerConfig.getConfig("committer");
        var consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer());

        var topicName = consumerConfig.getString("topic");
        var maxParallelism = 4;
        String result = Consumer
                .committableSource(consumerSettings, Subscriptions.topics(topicName))
                .idleTimeout(Duration.ofSeconds(5))
                .map(committableMessage -> {
                    System.out.printf("%s Message key %s, value %s, partition %d%n",
                            Thread.currentThread().getName(), committableMessage.record().key(),
                            committableMessage.record().value(), committableMessage.record().partition());
                    return committableMessage;
                })
                .mapAsync(maxParallelism, msg -> CompletableFuture.completedFuture(msg.committableOffset()))
                .runWith(Committer.sink(CommitterSettings.create(committerSettings)), materializer)
                .toCompletableFuture()
                .handle(AppSupport.doneHandler())
                .join();

        System.out.println("CommittableApp done with " + result);

        materializer.shutdown();
        system.terminate();
    }

}
