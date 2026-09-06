package com.fixflow.usecase2.si;

import com.fixflow.common.fix.FixParseException;
import com.fixflow.common.orders.OrderProcessor;
import com.fixflow.support.kafka.KafkaAcks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;

/**
 * The one handler class for both topics. It runs on the {@code uc2-handler-} worker threads, and acknowledges the
 * record once it is processed: the {@code kafka_acknowledgment} header belongs to the listener container of the
 * topic the record came from, so the commit goes back to {@code algo} or {@code dma} respectively.
 */
public class OrderHandler implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderHandler.class);

    private final OrderProcessor processor;

    public OrderHandler(OrderProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void handleMessage(Message<?> message) {
        String topic = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC);
        try {
            processor.process(topic, (String) message.getPayload());
        }
        catch (FixParseException e) {
            // poison-message policy: log, then acknowledge so that the partition is not blocked
            log.warn("Skipping invalid order from {}-{}@{}: {}", topic,
                    message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                    message.getHeaders().get(KafkaHeaders.OFFSET), e.getMessage());
        }
        KafkaAcks.acknowledge(message);
    }
}
