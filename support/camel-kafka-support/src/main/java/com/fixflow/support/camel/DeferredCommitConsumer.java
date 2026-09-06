package com.fixflow.support.camel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic proxy around a Kafka {@link Consumer} that also implements {@link DeferredCommits}.
 * <p>
 * After every {@code poll()} the delivered offsets are recorded as outstanding. Every {@code poll},
 * {@code unsubscribe} and {@code close} call, which by contract come from the consumer thread, first commits each
 * partition up to its first outstanding offset (or past the last delivered record when nothing is outstanding),
 * never moving a commit position backwards. Other threads never touch the delegate: {@link #acknowledge} only
 * updates a concurrent set. A proxy is used instead of a decorator class so that this code does not have to track
 * the (large, evolving) {@code Consumer} interface across kafka-clients versions.
 */
public final class DeferredCommitConsumer implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(DeferredCommitConsumer.class);

    /** Consumer-thread entry points that flush the acknowledged offsets before delegating. */
    private static final Set<String> FLUSH_BEFORE = Set.of("poll", "unsubscribe", "close");

    private final Consumer<?, ?> delegate;
    private final Duration commitTimeout;

    /** Per partition: offsets delivered by poll() that are not acknowledged yet. Updated from any thread. */
    private final ConcurrentMap<TopicPartition, ConcurrentSkipListSet<Long>> outstanding = new ConcurrentHashMap<>();

    /** Per partition: the last offset delivered by poll(). Consumer thread only. */
    private final Map<TopicPartition, Long> lastDelivered = new HashMap<>();

    /** Per partition: the position committed last. Consumer thread only. */
    private final Map<TopicPartition, Long> lastCommitted = new HashMap<>();

    private DeferredCommitConsumer(Consumer<?, ?> delegate, Duration commitTimeout) {
        this.delegate = delegate;
        this.commitTimeout = commitTimeout;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Consumer<K, V> wrap(Consumer<K, V> delegate, Duration commitTimeout) {
        return (Consumer<K, V>) Proxy.newProxyInstance(
                DeferredCommitConsumer.class.getClassLoader(),
                new Class<?>[] {Consumer.class, DeferredCommits.class},
                new DeferredCommitConsumer(delegate, commitTimeout));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> declaringClass = method.getDeclaringClass();
        if (declaringClass == DeferredCommits.class) {
            return switch (method.getName()) {
                case "acknowledge" -> {
                    acknowledge((TopicPartition) args[0], (Long) args[1]);
                    yield null;
                }
                case "outstanding" -> snapshotOutstanding();
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }
        if (declaringClass == Object.class) {
            return switch (method.getName()) {
                case "toString" -> "DeferredCommitConsumer(" + delegate + ")";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }
        if (FLUSH_BEFORE.contains(method.getName())) {
            flushAcknowledgedOffsets();
        }
        Object result;
        try {
            result = method.invoke(delegate, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
        if ("poll".equals(method.getName()) && result instanceof ConsumerRecords<?, ?> records) {
            trackDelivered(records);
        }
        return result;
    }

    private void acknowledge(TopicPartition partition, long offset) {
        ConcurrentSkipListSet<Long> offsets = outstanding.get(partition);
        if (offsets != null) {
            offsets.remove(offset);
        }
    }

    private Map<TopicPartition, SortedSet<Long>> snapshotOutstanding() {
        Map<TopicPartition, SortedSet<Long>> snapshot = new HashMap<>();
        outstanding.forEach((partition, offsets) -> snapshot.put(partition, new TreeSet<>(offsets)));
        return snapshot;
    }

    /** Runs on the consumer thread only. */
    private void trackDelivered(ConsumerRecords<?, ?> records) {
        Set<TopicPartition> assignment = delegate.assignment();
        outstanding.keySet().retainAll(assignment);
        lastDelivered.keySet().retainAll(assignment);
        lastCommitted.keySet().retainAll(assignment);
        for (TopicPartition partition : records.partitions()) {
            ConcurrentSkipListSet<Long> offsets = outstanding.computeIfAbsent(partition, p -> new ConcurrentSkipListSet<>());
            long last = -1;
            for (ConsumerRecord<?, ?> record : records.records(partition)) {
                // the position before the first delivery is where the consumer resumed from: nothing to commit yet
                lastCommitted.putIfAbsent(partition, record.offset());
                offsets.add(record.offset());
                last = record.offset();
            }
            lastDelivered.put(partition, last);
        }
    }

    /** Runs on the consumer thread only. */
    private void flushAcknowledgedOffsets() {
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        for (Map.Entry<TopicPartition, ConcurrentSkipListSet<Long>> entry : outstanding.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long delivered = lastDelivered.get(partition);
            if (delivered == null) {
                continue;
            }
            long position = firstOutstanding(entry.getValue(), delivered + 1);
            if (position > lastCommitted.getOrDefault(partition, -1L)) {
                commits.put(partition, new OffsetAndMetadata(position));
            }
        }
        if (commits.isEmpty()) {
            return;
        }
        try {
            delegate.commitSync(commits, commitTimeout);
            commits.forEach((partition, offset) -> lastCommitted.put(partition, offset.offset()));
            log.debug("Committed {}", commits);
        }
        catch (RuntimeException e) {
            // Not fatal: the records are redelivered after a restart or rebalance (at-least-once).
            log.warn("Commit of {} failed: {}", commits, e.toString());
        }
    }

    private static long firstOutstanding(ConcurrentSkipListSet<Long> offsets, long ifNone) {
        try {
            return offsets.first();
        }
        catch (NoSuchElementException e) {
            return ifNone;
        }
    }
}
