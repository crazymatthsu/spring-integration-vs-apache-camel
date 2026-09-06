package com.fixflow.usecase1.si;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Use case 1 with Spring Integration: FIX 4.2 NewOrderSingle messages are consumed from Kafka, batched
 * asynchronously, inserted into SQLite with one JDBC batch and acknowledged to Kafka only after the insert.
 */
@SpringBootApplication
public class Usecase1SpringIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(Usecase1SpringIntegrationApplication.class, args);
    }
}
