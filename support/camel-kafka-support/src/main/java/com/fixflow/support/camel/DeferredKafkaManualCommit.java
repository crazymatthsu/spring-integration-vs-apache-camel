package com.fixflow.support.camel;

import org.apache.camel.component.kafka.consumer.DefaultKafkaManualCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommitFactory;

/**
 * The {@code CamelKafkaManualCommit} header value handed to routes: {@link #commit()} parks the offset on the
 * consumer proxy instead of touching the {@code KafkaConsumer}, so it may be called from any thread.
 */
public final class DeferredKafkaManualCommit extends DefaultKafkaManualCommit {

    DeferredKafkaManualCommit(KafkaManualCommitFactory.CamelExchangePayload camelExchangePayload,
                              KafkaManualCommitFactory.KafkaRecordPayload kafkaRecordPayload) {
        super(camelExchangePayload, kafkaRecordPayload);
    }

    @Override
    public void commit() {
        if (!(camelExchangePayload.consumer instanceof DeferredCommits deferred)) {
            throw new IllegalStateException("The Kafka consumer is not wrapped for deferred commits: configure "
                    + DeferredCommitKafkaClientFactory.class.getSimpleName()
                    + " as the Kafka component's kafkaClientFactory");
        }
        deferred.acknowledge(getPartition(), getRecordOffset());
    }
}
