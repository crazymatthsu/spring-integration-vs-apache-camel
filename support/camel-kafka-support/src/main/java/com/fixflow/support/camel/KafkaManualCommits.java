package com.fixflow.support.camel;

import java.util.Collection;

import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;

/** Route-side helpers around the {@code CamelKafkaManualCommit} header. */
public final class KafkaManualCommits {

    private KafkaManualCommits() {
    }

    public static KafkaManualCommit from(Exchange exchange) {
        KafkaManualCommit manual = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manual == null) {
            throw new IllegalStateException(
                    "No " + KafkaConstants.MANUAL_COMMIT + " header: is allowManualCommit=true on the Kafka endpoint?");
        }
        return manual;
    }

    /** Acknowledges the record carried by {@code exchange}. */
    public static void commit(Exchange exchange) {
        from(exchange).commit();
    }

    /** Acknowledges every record of a batch. */
    public static void commitAll(Collection<Exchange> exchanges) {
        exchanges.forEach(KafkaManualCommits::commit);
    }
}
