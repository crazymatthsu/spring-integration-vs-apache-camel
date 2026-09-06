package com.fixflow.usecase1.camel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import javax.sql.DataSource;

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
 * in batches, and the consumer group's offsets are committed only after the insert.
 */
@RequiresKafka
@SpringBootTest
class OrdersBatchInsertIT {

    static final String TOPIC = TopicNames.unique("orders");
    static final String GROUP = TopicNames.unique("uc1-camel");
    static final int BATCH_SIZE = 25;
    static final Path DB = tempDb();

    static KafkaTestSupport kafka;

    @Autowired
    DataSource dataSource;

    @Autowired
    BatchStats batchStats;

    @Autowired
    OrdersRoute ordersRoute;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("camel.component.kafka.brokers", KafkaTestSupport::bootstrapServers);
        registry.add("fixflow.orders.topic", () -> TOPIC);
        registry.add("fixflow.orders.group-id", () -> GROUP);
        registry.add("fixflow.orders.batch-size", () -> BATCH_SIZE);
        registry.add("fixflow.orders.batch-timeout", () -> "300ms");
        registry.add("fixflow.db.path", DB::toString);
    }

    @BeforeAll
    static void startKafkaClients() {
        kafka = new KafkaTestSupport();
        kafka.createTopics(3, TOPIC);
    }

    @AfterAll
    static void stopKafkaClients() {
        kafka.deleteTopics(TOPIC);
        kafka.close();
    }

    @Test
    void ordersAreBatchInsertedAndAcknowledgedAfterTheInsert() {
        FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");
        int validOrders = 120;
        for (int i = 1; i <= validOrders; i++) {
            String clOrdId = "ORD-" + i;
            kafka.send(TOPIC, clOrdId, factory.newOrderSingle(clOrdId, "AAPL", i % 2 == 0 ? '1' : '2',
                    BigDecimal.valueOf(100L * i), i % 5 == 0 ? null : new BigDecimal("189.50")));
        }
        // two poison records: valid FIX that is not an order, and garbage
        kafka.send(TOPIC, "hb", factory.heartbeat());
        kafka.send(TOPIC, "junk", "this is not FIX");
        int totalRecords = validOrders + 2;

        OrderRepository repository = new OrderRepository(dataSource);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(repository.count()).isEqualTo(validOrders));

        // every record (poison ones included) is eventually acknowledged, i.e. committed for the group
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(kafka.committedOffsetTotal(GROUP, TOPIC)).isEqualTo(totalRecords));

        assertThat(repository.findAllClOrdIds()).hasSize(validOrders).doesNotHaveDuplicates();
        assertThat(ordersRoute.skipped()).isEqualTo(2);
        assertThat(batchStats.rows()).isEqualTo(validOrders);
        assertThat(batchStats.largestBatch()).isBetween(2, BATCH_SIZE);
        assertThat(batchStats.batches()).isLessThan(validOrders);
    }

    private static Path tempDb() {
        try {
            return Files.createTempDirectory("uc1-camel").resolve("orders.db");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
