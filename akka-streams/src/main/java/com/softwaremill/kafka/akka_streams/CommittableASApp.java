package com.softwaremill.kafka.akka_streams;

import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class CommittableASApp {

    public static void main(String[] args) {
        var system = ActorSystem.create("KafkaAkkaStreams");
        var materializer = ActorMaterializer.create(system);
        var configuration = system.settings().config();

        var consumerConfig = configuration.getConfig("our-kafka-consumer");
        var committerSettings = consumerConfig.getConfig("committer");
        var consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer());

        var topicName = consumerConfig.getString("topic");
        String result = Consumer
                .committableSource(consumerSettings, Subscriptions.topics(topicName))
                .idleTimeout(Duration.ofSeconds(5))
                .log("received", committableMessage -> "Message key " + committableMessage.record().key() + ", value " + committableMessage.record().value())
                .withAttributes(
                        Attributes.createLogLevels(
                                Logging.WarningLevel(), // onElement
                                Logging.InfoLevel(), // onFinish
                                Logging.DebugLevel() // onFailure
                        ))
                .mapAsync(4, msg -> CompletableFuture.completedFuture(msg.committableOffset()))
                .runWith(Committer.sink(CommitterSettings.create(committerSettings)), materializer)
                .toCompletableFuture()
                .handle(AppSupport.doneHandler())
                .join();

        System.out.println("Done with " + result);

        materializer.shutdown();
        system.terminate();
    }

}
