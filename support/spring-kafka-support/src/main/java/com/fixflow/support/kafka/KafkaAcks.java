package com.fixflow.support.kafka;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/** Access to the {@link Acknowledgment} that the Kafka inbound adapter puts on every message in manual ack mode. */
public final class KafkaAcks {

    private KafkaAcks() {
    }

    public static Acknowledgment from(MessageHeaders headers) {
        Acknowledgment ack = headers.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            throw new IllegalStateException(
                    "No " + KafkaHeaders.ACKNOWLEDGMENT + " header: is the listener container in AckMode.MANUAL?");
        }
        return ack;
    }

    public static void acknowledge(Message<?> message) {
        from(message.getHeaders()).acknowledge();
    }
}
