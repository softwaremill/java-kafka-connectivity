package com.softwaremill.kafka.akka_streams;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

public class PartitionedASApp {

    public static void main(String[] args) {
        var system = ActorSystem.create("KafkaAkkaStreams");
        var materializer = ActorMaterializer.create(system);

        var configuration = system.settings().config();

        var maxPartitions = 5;

        var consumerConfig = configuration.getConfig("our-kafka-consumer");
        var consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer());

        String topicName = consumerConfig.getString("topic");
        String result = Consumer
                .committablePartitionedSource(consumerSettings, Subscriptions.topics(topicName))
                .idleTimeout(Duration.ofSeconds(5))
                .mapAsyncUnordered(maxPartitions, pair -> {
                    Source<ConsumerMessage.CommittableMessage<String, String>, NotUsed> source = pair.second();
                    return source
                            .log("mapped", committableMessage ->
                                    String.format("Message key %s, value %s, partition %d",
                                            committableMessage.record().key(), committableMessage.record().value(), committableMessage.record().partition()))
                            .withAttributes(
                                    Attributes.createLogLevels(
                                            Logging.InfoLevel(), // onElement
                                            Logging.InfoLevel(), // onFinish
                                            Logging.DebugLevel() // onFailure
                                    ))
                            .map(ConsumerMessage.CommittableMessage::committableOffset)
                            .runWith(Committer.sink(CommitterSettings.create(system)), materializer);
                })
                .runWith(Sink.ignore(), materializer)
                .toCompletableFuture()
                .handle(AppSupport.doneHandler())
                .join();

        System.out.println("Done with " + result);

        materializer.shutdown();
        system.terminate();
    }

}
