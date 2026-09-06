package com.fixflow.common.testing;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Thin wrapper around the Kafka admin and producer clients for integration tests.
 * Talks to the broker started by {@code podman compose} (see {@link #bootstrapServers()}).
 */
public final class KafkaTestSupport implements AutoCloseable {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final Admin admin;
    private final KafkaProducer<String, String> producer;

    public KafkaTestSupport() {
        String bootstrap = bootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.admin = Admin.create(adminProps);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        this.producer = new KafkaProducer<>(producerProps);
    }

    /** {@code -Dfixflow.kafka.bootstrap}, then {@code $FIXFLOW_KAFKA_BOOTSTRAP}, then {@code localhost:9092}. */
    public static String bootstrapServers() {
        return System.getProperty("fixflow.kafka.bootstrap",
                System.getenv().getOrDefault("FIXFLOW_KAFKA_BOOTSTRAP", "localhost:9092"));
    }

    public void createTopics(int partitions, String... topics) {
        List<NewTopic> newTopics = Arrays.stream(topics)
                .map(topic -> new NewTopic(topic, partitions, (short) 1))
                .toList();
        try {
            admin.createTopics(newTopics).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot create topics " + Arrays.toString(topics), e);
        }
    }

    /** Best-effort clean-up. */
    public void deleteTopics(String... topics) {
        try {
            admin.deleteTopics(Arrays.asList(topics)).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException | TimeoutException e) {
            // ignored: the topic names are unique per run anyway
        }
    }

    /** Sends synchronously so that the test can rely on the record being in the log. */
    public RecordMetadata send(String topic, String key, String value) {
        try {
            return producer.send(new ProducerRecord<>(topic, key, value)).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot send to " + topic, e);
        }
    }

    /** Committed offset per partition of {@code topic} for the consumer group (partitions without a commit are absent). */
    public Map<TopicPartition, Long> committedOffsets(String groupId, String topic) {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            Map<TopicPartition, Long> result = new HashMap<>();
            offsets.forEach((tp, om) -> {
                if (tp.topic().equals(topic) && om != null) {
                    result.put(tp, om.offset());
                }
            });
            return result;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot read committed offsets of group " + groupId, e);
        }
    }

    /** Sum of the committed offsets over all partitions of {@code topic}, i.e. the number of acknowledged records. */
    public long committedOffsetTotal(String groupId, String topic) {
        return committedOffsets(groupId, topic).values().stream().mapToLong(Long::longValue).sum();
    }

    /** Sum of the log-end offsets over all partitions of {@code topic}, i.e. the number of records in the topic. */
    public long endOffsetTotal(String topic) {
        try {
            TopicDescription description = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).get(topic);
            Map<TopicPartition, OffsetSpec> request = description.partitions().stream()
                    .collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                    admin.listOffsets(request).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            return offsets.values().stream().mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset).sum();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot read end offsets of " + topic, e);
        }
    }

    @Override
    public void close() {
        producer.close(TIMEOUT);
        admin.close(TIMEOUT);
    }
}
