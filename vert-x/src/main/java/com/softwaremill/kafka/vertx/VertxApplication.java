package com.softwaremill.kafka.vertx;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;

import static com.softwaremill.kafka.vertx.ConsumerType.SINGLE;
import static java.util.stream.Collectors.toMap;

@Slf4j
public class VertxApplication extends AbstractVerticle {

    @Override
    public void start() {
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("properties")
                .setConfig(new JsonObject().put("path", "vertx.properties").put("raw-data", true));
        ConfigStoreOptions envStore = new ConfigStoreOptions()
                .setType("sys");
        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(envStore);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(json -> {
            JsonObject result = json.result();
            String topic = result.getString("consumer.topic");
            Map<String, String> kafkaConfig = getKafkaConfig(result);

            ConsumerType type = ConsumerType.valueOf(
                    result.getString("consumer.type").toUpperCase());

            DeploymentOptions deploymentOptions = workerDeploymentOptions();
            if (SINGLE == type) {
                runSingleVertex(topic, kafkaConfig, deploymentOptions);
            } else {
                runPartitionedVertices(topic, kafkaConfig, deploymentOptions);
            }
        });
    }

    private void runSingleVertex(String topic, Map<String, String> kafkaConfig, DeploymentOptions deploymentOptions) {
        vertx.deployVerticle(
                () -> KafkaVerticle.create(topic, kafkaConfig),
                deploymentOptions,
                async -> log.info("Kafka consumer deployed. DeploymentId: {}", async.result())
        );
    }

    private void runPartitionedVertices(String topic, Map<String, String> kafkaConfig, DeploymentOptions deploymentOptions) {
        Map<String, Object> adminClientConfig = kafkaConfig
                .entrySet().stream()
                .map(e -> new SimpleImmutableEntry<String, Object>(e.getKey(), e.getValue()))
                .collect(toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
        KafkaAdminClient.create(adminClientConfig)
                .describeTopics(Collections.singletonList(topic))
                .values()
                .get(topic)
                .whenComplete(
                        (description, exc) -> {
                            if (description != null) {
                                int numberOfPartitions = description.partitions().size();
                                deployPartitionedVertices(topic, kafkaConfig, deploymentOptions, numberOfPartitions);
                            }
                            if (exc != null) {
                                log.error("Error when calling for topic {} description", topic, exc);
                            }
                        }
                );

    }

    private void deployPartitionedVertices(String topic, Map<String, String> kafkaConfig, DeploymentOptions deploymentOptions, int numberOfPartitions) {
        IntStream.range(0, numberOfPartitions)
                .forEach(partition -> {
                    vertx.deployVerticle(
                            () -> KafkaPartitionedVerticle.create(topic, partition, kafkaConfig),
                            deploymentOptions,
                            async -> log.info("Kafka consumer deployed. DeploymentId: {}", async.result())
                    );
                });
    }

    private Map<String, String> getKafkaConfig(JsonObject result) {
        return result
                .fieldNames()
                .stream()
                .filter(name -> ConsumerConfig.configNames().contains(name))
                .map(key -> new SimpleImmutableEntry<>(key, result.getString(key)))
                .collect(toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
    }

    private DeploymentOptions workerDeploymentOptions() {
        return new DeploymentOptions().setWorker(true);
    }

}
