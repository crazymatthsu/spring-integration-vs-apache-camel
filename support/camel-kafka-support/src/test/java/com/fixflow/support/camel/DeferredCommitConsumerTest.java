package com.fixflow.support.camel;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeferredCommitConsumerTest {

    private final TopicPartition partition = new TopicPartition("orders", 0);

    /** Every synchronous commit the proxy issues, keyed by partition (MockConsumer forgets them on unsubscribe/close). */
    private final Map<TopicPartition, Long> commits = new HashMap<>();

    private final MockConsumer<String, String> mock = new MockConsumer<>("earliest") {
        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            offsets.forEach((tp, om) -> commits.put(tp, om.offset()));
            super.commitSync(offsets, timeout);
        }
    };

    private final Consumer<String, String> consumer = DeferredCommitConsumer.wrap(mock, Duration.ofSeconds(5));
    private final DeferredCommits deferred = (DeferredCommits) consumer;

    @BeforeEach
    void assign() {
        mock.assign(List.of(partition));
        mock.updateBeginningOffsets(Map.of(partition, 0L));
    }

    @Test
    void commitsOnlyTheContiguousAcknowledgedPrefixOfAPartition() throws InterruptedException {
        deliver(0, 9);
        assertThat(deferred.outstanding().get(partition)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        assertThat(commits).isEmpty();

        acknowledgeOnWorkerThread(0, 1, 2, 5); // out of order: 5 is done, 3 and 4 are not
        consumer.poll(Duration.ZERO);          // what the consumer thread does
        assertThat(commits).containsEntry(partition, 3L);
        assertThat(deferred.outstanding().get(partition)).containsExactly(3L, 4L, 6L, 7L, 8L, 9L);

        acknowledgeOnWorkerThread(3, 4);
        consumer.poll(Duration.ZERO);
        assertThat(commits).containsEntry(partition, 6L);

        acknowledgeOnWorkerThread(6, 7, 8, 9);
        consumer.poll(Duration.ZERO);
        assertThat(commits).containsEntry(partition, 10L);
        assertThat(deferred.outstanding().get(partition)).isEmpty();
    }

    @Test
    void nothingIsCommittedWhileTheOldestRecordIsStillInFlight() throws InterruptedException {
        deliver(0, 4);
        acknowledgeOnWorkerThread(1, 2, 3, 4);

        consumer.poll(Duration.ZERO);

        assertThat(commits).isEmpty();
    }

    @Test
    void acknowledgedOffsetsAreFlushedBeforeUnsubscribe() throws InterruptedException {
        deliver(0, 2);
        acknowledgeOnWorkerThread(0, 1, 2);

        consumer.unsubscribe();

        assertThat(commits).containsEntry(partition, 3L);
    }

    @Test
    void acknowledgedOffsetsAreFlushedBeforeClose() throws InterruptedException {
        deliver(0, 1);
        acknowledgeOnWorkerThread(0, 1);

        consumer.close();

        assertThat(commits).containsEntry(partition, 2L);
    }

    @Test
    void concurrentAcknowledgmentsFromManyThreads() throws InterruptedException {
        deliver(0, 199);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int offset = 0; offset < 200; offset++) {
            long done = offset;
            pool.execute(() -> deferred.acknowledge(partition, done));
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        consumer.poll(Duration.ZERO);

        assertThat(commits).containsEntry(partition, 200L);
    }

    @Test
    void behavesLikeAnOrdinaryConsumerOtherwise() {
        assertThat(consumer.assignment()).containsExactly(partition);
        assertThat(consumer.toString()).startsWith("DeferredCommitConsumer(");
        assertThat(consumer).isEqualTo(consumer);
        assertThat(consumer.hashCode()).isEqualTo(consumer.hashCode());
        assertThat(commits).isEmpty();
    }

    /** Adds the records to the mock and polls them, as the consumer thread would. */
    private void deliver(int fromOffset, int toOffset) {
        for (int offset = fromOffset; offset <= toOffset; offset++) {
            mock.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, "k" + offset, "v"));
        }
        assertThat(consumer.poll(Duration.ZERO).count()).isEqualTo(toOffset - fromOffset + 1);
    }

    private void acknowledgeOnWorkerThread(long... offsets) throws InterruptedException {
        Thread worker = new Thread(() -> {
            for (long offset : offsets) {
                deferred.acknowledge(partition, offset);
            }
        });
        worker.start();
        worker.join();
    }
}
