package com.fixflow.usecase1.si;

import java.util.concurrent.atomic.AtomicLong;

import com.fixflow.support.kafka.KafkaAcks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

/**
 * Poison-message policy: a record that is not a valid FIX order is logged and acknowledged, so that it does not
 * block the commit of every later record of its partition (out-of-order acks only commit contiguous offsets).
 */
public class InvalidOrderHandler implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(InvalidOrderHandler.class);

    private final AtomicLong skipped = new AtomicLong();

    @Override
    public void handleMessage(Message<?> errorMessage) {
        if (errorMessage.getPayload() instanceof MessagingException failure && failure.getFailedMessage() != null) {
            Message<?> failed = failure.getFailedMessage();
            Throwable cause = NestedExceptionUtils.getMostSpecificCause(failure);
            log.warn("Skipping invalid order from {}-{}@{}: {}",
                    failed.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                    failed.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                    failed.getHeaders().get(KafkaHeaders.OFFSET),
                    cause.getMessage());
            KafkaAcks.acknowledge(failed);
            skipped.incrementAndGet();
        }
        else {
            log.error("Unexpected error message, nothing acknowledged: {}", errorMessage);
        }
    }

    public long skipped() {
        return skipped.get();
    }
}
