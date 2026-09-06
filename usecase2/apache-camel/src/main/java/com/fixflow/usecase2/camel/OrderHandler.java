package com.fixflow.usecase2.camel;

import com.fixflow.common.fix.FixParseException;
import com.fixflow.common.orders.OrderProcessor;
import com.fixflow.support.camel.KafkaManualCommits;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one handler class for both topics. It runs on the seda consumer threads, and acknowledges the record once it
 * is processed: the {@code CamelKafkaManualCommit} header belongs to the Kafka consumer of the topic the record came
 * from, so the commit goes back to {@code algo} or {@code dma} respectively.
 */
public class OrderHandler implements Processor {

    private static final Logger log = LoggerFactory.getLogger(OrderHandler.class);

    private final OrderProcessor processor;

    public OrderHandler(OrderProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void process(Exchange exchange) {
        String topic = exchange.getMessage().getHeader(KafkaConstants.TOPIC, String.class);
        try {
            processor.process(topic, exchange.getMessage().getBody(String.class));
        }
        catch (FixParseException e) {
            // poison-message policy: log, then acknowledge so that the partition is not blocked
            log.warn("Skipping invalid order from {}-{}@{}: {}", topic,
                    exchange.getMessage().getHeader(KafkaConstants.PARTITION),
                    exchange.getMessage().getHeader(KafkaConstants.OFFSET), e.getMessage());
        }
        KafkaManualCommits.commit(exchange);
    }
}
