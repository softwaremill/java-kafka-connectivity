Java Kafka connectivity
===============


Plain Java
---------------


Spring Boot
---------------


Micronaut
---------------



Akka Streams
---------------


Vert.x
---------------
Kafka vertices are implemented by [vertx-kafka-client](https://vertx.io/docs/vertx-kafka-client/java/) component.

Build project using goal from [vertx-gradle-plugin](https://github.com/jponge/vertx-gradle-plugin):
```./gradlew :vert-x:build```

You can then start the application by calling  `java -jar vert-x-1.0-all.jar`. It starts single-threaded Kafka consumer. You can run thread-per-partition consumer by providing `-Dconsumer.type=multi` parameter. 
