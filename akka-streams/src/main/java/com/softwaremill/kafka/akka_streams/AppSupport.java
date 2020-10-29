package com.softwaremill.kafka.akka_streams;

import akka.Done;

import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

class AppSupport {

    static BiFunction<Done, Throwable, String> doneHandler() {
        return (done, exc) -> {
            if (done != null) {
                System.out.println("Completed successfully");
                return "ok";
            } else if (exc instanceof TimeoutException) {
                System.out.println("Completed with timeout " + exc);
                return "timeout";
            } else {
                System.out.println("Completed with error " + exc);
                return "error";
            }
        };
    }

}
