package com.fixflow.usecase1.camel;

import com.fixflow.support.camel.DeferredCommitConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Use case 1 with Apache Camel: FIX 4.2 NewOrderSingle messages are consumed from Kafka, batched
 * asynchronously, inserted into SQLite with one JDBC batch and acknowledged to Kafka only after the insert.
 */
@SpringBootApplication
@Import(DeferredCommitConfiguration.class)
public class Usecase1ApacheCamelApplication {

    public static void main(String[] args) {
        SpringApplication.run(Usecase1ApacheCamelApplication.class, args);
    }
}
