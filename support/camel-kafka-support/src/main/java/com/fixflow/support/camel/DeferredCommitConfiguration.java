package com.fixflow.support.camel;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the two factories the Kafka component needs for deferred manual commits.
 * Camel Spring Boot auto-wires single beans of these types into the component; the demos also reference them
 * explicitly through {@code camel.component.kafka.kafka-client-factory} / {@code kafka-manual-commit-factory}.
 */
@Configuration(proxyBeanMethods = false)
public class DeferredCommitConfiguration {

    @Bean
    public DeferredCommitKafkaClientFactory deferredCommitKafkaClientFactory(
            @Value("${camel.component.kafka.commit-timeout-ms:5000}") long commitTimeoutMs) {
        return new DeferredCommitKafkaClientFactory(Duration.ofMillis(commitTimeoutMs));
    }

    @Bean
    public DeferredKafkaManualCommitFactory deferredKafkaManualCommitFactory() {
        return new DeferredKafkaManualCommitFactory();
    }
}
