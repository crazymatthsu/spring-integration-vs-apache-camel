package com.fixflow.support.camel;

import java.time.Duration;
import java.util.Properties;

import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Creates consumers wrapped by {@link DeferredCommitConsumer}
 * ({@code camel.component.kafka.kafka-client-factory}). Must be paired with {@link DeferredKafkaManualCommitFactory}.
 */
public final class DeferredCommitKafkaClientFactory extends DefaultKafkaClientFactory {

    private final Duration commitTimeout;

    public DeferredCommitKafkaClientFactory(Duration commitTimeout) {
        this.commitTimeout = commitTimeout;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Consumer getConsumer(Properties kafkaProps) {
        return DeferredCommitConsumer.wrap(super.getConsumer(kafkaProps), commitTimeout);
    }
}
