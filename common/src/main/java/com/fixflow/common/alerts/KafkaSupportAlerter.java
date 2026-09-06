package com.fixflow.common.alerts;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes every alert to a Kafka topic (one text record, keyed by the source application), so that on-call
 * tooling, a dashboard or a ticketing bridge can subscribe to it. Publishing is asynchronous and never throws:
 * a broker problem must not turn a data problem into a processing failure.
 */
public final class KafkaSupportAlerter implements SupportAlerter, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSupportAlerter.class);

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public KafkaSupportAlerter(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "support-alerter");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        this.producer = new KafkaProducer<>(props);
    }

    public String topic() {
        return topic;
    }

    @Override
    public void raise(SupportAlert alert) {
        try {
            producer.send(new ProducerRecord<>(topic, alert.source(), alert.toLogLine()), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Could not publish support alert to {}: {}", topic, exception.toString());
                }
            });
        }
        catch (RuntimeException e) {
            log.error("Could not publish support alert to {}: {}", topic, e.toString());
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(10));
    }
}
