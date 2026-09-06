package com.fixflow.usecase2.camel;

import java.math.BigDecimal;
import java.time.Duration;

import com.fixflow.common.fix.FixMessageFactory;
import com.fixflow.common.orders.OrderProcessor;
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
 * End-to-end against the Kafka broker from podman compose: orders from both topics are handled by the shared
 * handler on worker threads, and each topic's offsets are committed once its records are processed.
 */
@RequiresKafka
@SpringBootTest
class DualTopicHandlerIT {

    static final String ALGO = TopicNames.unique("algo");
    static final String DMA = TopicNames.unique("dma");
    static final String GROUP = TopicNames.unique("uc2-camel");

    static KafkaTestSupport kafka;

    @Autowired
    OrderProcessor orderProcessor;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("camel.component.kafka.brokers", KafkaTestSupport::bootstrapServers);
        registry.add("fixflow.sources.algo-topic", () -> ALGO);
        registry.add("fixflow.sources.dma-topic", () -> DMA);
        registry.add("fixflow.sources.group-id", () -> GROUP);
    }

    @BeforeAll
    static void startKafkaClients() {
        kafka = new KafkaTestSupport();
        kafka.createTopics(3, ALGO, DMA);
    }

    @AfterAll
    static void stopKafkaClients() {
        kafka.deleteTopics(ALGO, DMA);
        kafka.close();
    }

    @Test
    void ordersFromBothTopicsAreHandledOnWorkerThreadsAndAcknowledgedToTheirTopic() {
        FixMessageFactory algoClient = new FixMessageFactory("ALGO1", "BROKER");
        FixMessageFactory dmaClient = new FixMessageFactory("DMA1", "BROKER");
        int algoOrders = 60;
        int dmaOrders = 40;
        for (int i = 1; i <= algoOrders; i++) {
            kafka.send(ALGO, "ALGO-" + i, algoClient.newOrderSingle("ALGO-" + i, "MSFT", '1',
                    BigDecimal.valueOf(100L * i), new BigDecimal("410.25")));
        }
        for (int i = 1; i <= dmaOrders; i++) {
            kafka.send(DMA, "DMA-" + i, dmaClient.newOrderSingle("DMA-" + i, "NVDA", '2',
                    BigDecimal.valueOf(10L * i), null));
        }
        // one poison record per topic
        kafka.send(ALGO, "junk", "this is not FIX");
        kafka.send(DMA, "hb", dmaClient.heartbeat());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(orderProcessor.processed(ALGO)).isEqualTo(algoOrders);
            assertThat(orderProcessor.processed(DMA)).isEqualTo(dmaOrders);
        });
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(kafka.committedOffsetTotal(GROUP, ALGO)).isEqualTo(algoOrders + 1);
            assertThat(kafka.committedOffsetTotal(GROUP, DMA)).isEqualTo(dmaOrders + 1);
        });

        assertThat(orderProcessor.failed(ALGO)).isEqualTo(1);
        assertThat(orderProcessor.failed(DMA)).isEqualTo(1);
        // the handler never ran on a Kafka consumer thread
        assertThat(orderProcessor.threadNames()).isNotEmpty()
                .allSatisfy(name -> assertThat(name).contains("seda://orders"));
    }
}
