package com.fixflow.usecase1.si;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import javax.sql.DataSource;

import com.fixflow.common.alerts.InsertFailure;
import com.fixflow.common.alerts.LoggingSupportAlerter;
import com.fixflow.common.alerts.SupportAlert;
import com.fixflow.common.fix.FixMessageFactory;
import com.fixflow.common.orders.BatchStats;
import com.fixflow.common.orders.OrderRepository;
import com.fixflow.common.testing.KafkaTestSupport;
import com.fixflow.common.testing.RequiresKafka;
import com.fixflow.common.testing.TopicNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end against the Kafka broker from podman compose: orders published to a fresh topic end up in SQLite
 * in batches, replayed orders make the batch fall back to one-by-one inserts and raise a support alert, and the
 * consumer group's offsets are committed only after the insert.
 */
@RequiresKafka
@SpringBootTest
class OrdersBatchInsertIT {

    static final String TOPIC = TopicNames.unique("orders");
    static final String ALERTS = TopicNames.unique("support-alerts");
    static final String GROUP = TopicNames.unique("uc1-si");
    static final int BATCH_SIZE = 25;
    static final Path DB = tempDb();

    static KafkaTestSupport kafka;

    @Autowired
    DataSource dataSource;

    @Autowired
    BatchStats batchStats;

    @Autowired
    InvalidOrderHandler invalidOrderHandler;

    @Autowired
    LoggingSupportAlerter supportAlerter;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KafkaTestSupport::bootstrapServers);
        registry.add("fixflow.orders.topic", () -> TOPIC);
        registry.add("fixflow.orders.group-id", () -> GROUP);
        registry.add("fixflow.orders.batch-size", () -> BATCH_SIZE);
        registry.add("fixflow.orders.batch-timeout", () -> "300ms");
        registry.add("fixflow.alerts.topic", () -> ALERTS);
        registry.add("fixflow.db.path", DB::toString);
    }

    @BeforeAll
    static void startKafkaClients() {
        kafka = new KafkaTestSupport();
        kafka.createTopics(3, TOPIC, ALERTS);
    }

    @AfterAll
    static void stopKafkaClients() {
        kafka.deleteTopics(TOPIC, ALERTS);
        kafka.close();
    }

    @Test
    void ordersAreBatchInsertedAndAcknowledgedAfterTheInsert() {
        FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");
        int validOrders = 120;
        for (int i = 1; i <= validOrders; i++) {
            kafka.send(TOPIC, "ORD-" + i, order(factory, i));
        }
        // two replayed orders: same sender and ClOrdID as before, a duplicate key for the unique index
        kafka.send(TOPIC, "ORD-7", order(factory, 7));
        kafka.send(TOPIC, "ORD-8", order(factory, 8));
        // two poison records: valid FIX that is not an order, and garbage
        kafka.send(TOPIC, "hb", factory.heartbeat());
        kafka.send(TOPIC, "junk", "this is not FIX");
        int totalRecords = validOrders + 2 + 2;

        OrderRepository repository = new OrderRepository(dataSource);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(repository.count()).isEqualTo(validOrders));

        // every record (replayed and poison ones included) is eventually acknowledged, i.e. committed for the group
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(kafka.committedOffsetTotal(GROUP, TOPIC)).isEqualTo(totalRecords));

        assertThat(repository.findAllClOrdIds()).hasSize(validOrders).doesNotHaveDuplicates();
        assertThat(invalidOrderHandler.skipped()).isEqualTo(2);
        assertThat(batchStats.rows()).isEqualTo(totalRecords - 2);
        assertThat(batchStats.largestBatch()).isBetween(2, BATCH_SIZE);
        assertThat(batchStats.batches()).isLessThan(validOrders);

        // the replayed orders made their batch fall back to one-by-one inserts, and support was told which rows failed
        assertThat(batchStats.fallbacks()).isGreaterThanOrEqualTo(1);
        assertThat(batchStats.failedRows()).isEqualTo(2);
        assertThat(supportAlerter.recent())
                .flatExtracting(SupportAlert::failures)
                .extracting(InsertFailure::clOrdId)
                .containsExactlyInAnyOrder("ORD-7", "ORD-8");
        assertThat(supportAlerter.recent()).allSatisfy(alert -> {
            assertThat(alert.source()).isEqualTo("usecase1-spring-integration");
            assertThat(alert.summary()).contains("could not be inserted");
        });
        // ... and the alert was published to the support topic as well
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(kafka.endOffsetTotal(ALERTS)).isEqualTo(supportAlerter.recent().size()));
    }

    private static String order(FixMessageFactory factory, int i) {
        return factory.newOrderSingle("ORD-" + i, "AAPL", i % 2 == 0 ? '1' : '2',
                BigDecimal.valueOf(100L * i), i % 5 == 0 ? null : new BigDecimal("189.50"));
    }

    private static Path tempDb() {
        try {
            return Files.createTempDirectory("uc1-si").resolve("orders.db");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
