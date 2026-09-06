package com.fixflow.support.camel;

import java.util.Map;
import java.util.SortedSet;

import org.apache.kafka.common.TopicPartition;

/**
 * Implemented by the consumer proxies created by {@link DeferredCommitKafkaClientFactory}.
 * <p>
 * A {@code KafkaConsumer} may only be used by the thread that polls it. Camel's Kafka consumer polls and runs the
 * route on that thread, so once an exchange has been handed to another thread (aggregator completion, seda, threads)
 * calling {@code KafkaManualCommit.commit()} there fails with {@code ConcurrentModificationException}.
 * <p>
 * {@link #acknowledge} is safe from any thread. The proxy remembers every offset delivered by {@code poll()} and,
 * on the consumer thread right before the next {@code poll()} (and before {@code unsubscribe()} / {@code close()}),
 * commits each partition up to its first unacknowledged record. Acknowledging out of order therefore never commits
 * past a record that is still being processed, which keeps at-least-once semantics.
 */
public interface DeferredCommits {

    /** Marks the record at {@code offset} of {@code partition} as processed. Safe from any thread. */
    void acknowledge(TopicPartition partition, long offset);

    /** Offsets delivered by {@code poll()} that have not been acknowledged yet, per partition (a snapshot). */
    Map<TopicPartition, SortedSet<Long>> outstanding();
}
