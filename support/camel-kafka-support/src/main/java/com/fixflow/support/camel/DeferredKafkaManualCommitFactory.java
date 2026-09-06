package com.fixflow.support.camel;

import org.apache.camel.component.kafka.consumer.CommitManager;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommitFactory;

/**
 * Plugs {@link DeferredKafkaManualCommit} into the Kafka component
 * ({@code camel.component.kafka.kafka-manual-commit-factory}). Must be paired with
 * {@link DeferredCommitKafkaClientFactory}.
 */
public final class DeferredKafkaManualCommitFactory implements KafkaManualCommitFactory {

    @Override
    public KafkaManualCommit newInstance(CamelExchangePayload camelExchangePayload,
                                         KafkaRecordPayload kafkaRecordPayload,
                                         CommitManager commitManager) {
        return new DeferredKafkaManualCommit(camelExchangePayload, kafkaRecordPayload);
    }
}
