Java Kafka connectivity
===============

Before running any application or Spring test producing sample messages, run docker-compose. It starts a local Kafka deployment.

Plain Java
---------------
Run plain Kafka consumer by starting `KafkaConsumerApp`. Configuration is held by `application.properties` file. 

Spring Boot
---------------
You can start it using `BootApplication` class. Various containers and message listeners have some parts of the code commented out, so only one type of listener can be started at once.

`test` package contains a sample test running a simple Kafka producer to feed required topic with some (partitioned) data.

Micronaut
---------------
Micronaut version of Kafka consumer can be started by running `MicronautApplicaiton` class. As in Sppring Boot example, here you can find some code commented out as well.

Akka Streams
---------------
Implementation for Akka Streams contains three apps.

`PlainRecordApp` uses a plain source mechanism, consuming records one by one on the same thread.

`PartitionedApp` process messages in parallel, with a dedicated thread per partition.

`CommittableApp` provides an example of an application with manual commits.

Vert.x
---------------
Kafka vertices are implemented by [vertx-kafka-client](https://vertx.io/docs/vertx-kafka-client/java/) component.

Build project using goal from [vertx-gradle-plugin](https://github.com/jponge/vertx-gradle-plugin):
```./gradlew :vert-x:build```

You can then start the application by calling  `java -jar vert-x-1.0-all.jar`. It starts single-threaded Kafka consumer. You can run thread-per-partition consumer by providing `-Dconsumer.type=multi` parameter. 
