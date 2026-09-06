package com.fixflow.usecase2.si;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Use case 2 with Spring Integration: FIX 4.2 NewOrderSingle messages from the {@code algo} and {@code dma} topics
 * are published to an in-memory channel, handled asynchronously by one shared handler class and acknowledged back
 * to the topic they came from.
 */
@SpringBootApplication
public class Usecase2SpringIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(Usecase2SpringIntegrationApplication.class, args);
    }
}
