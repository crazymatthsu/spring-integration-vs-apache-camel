package com.fixflow.usecase2.camel;

import com.fixflow.support.camel.DeferredCommitConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Use case 2 with Apache Camel: FIX 4.2 NewOrderSingle messages from the {@code algo} and {@code dma} topics
 * are published to an in-memory seda queue, handled asynchronously by one shared handler class and acknowledged
 * back to the topic they came from.
 */
@SpringBootApplication
@Import(DeferredCommitConfiguration.class)
public class Usecase2ApacheCamelApplication {

    public static void main(String[] args) {
        SpringApplication.run(Usecase2ApacheCamelApplication.class, args);
    }
}
