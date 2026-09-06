package com.fixflow.usecase1.camel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.FixParseException;
import com.fixflow.common.fix.NewOrder;
import com.fixflow.common.orders.BatchStats;
import com.fixflow.common.orders.OrderSql;
import com.fixflow.support.camel.KafkaManualCommits;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.processor.aggregate.GroupedExchangeAggregationStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * The Camel route of use case 1:
 * <pre>
 *  kafka (manual commit) -> process (FIX -> NewOrder) -> aggregate (completionSize | completionTimeout, own thread)
 *      -> sql (batch INSERT) -> acknowledge every record of the batch
 * </pre>
 * The Kafka consumer thread parses and drops the record into the aggregator; the completed batch is processed on
 * the {@code uc1-batch} thread. Acknowledgments issued there are parked and committed by the consumer thread before
 * its next poll, contiguous offsets only (see {@code camel-kafka-support}).
 */
@Component
public class OrdersRoute extends RouteBuilder {

    /** Exchange property holding the original record exchanges of the batch while the body is the SQL parameter list. */
    static final String BATCH_RECORDS = "fixflowBatchRecords";

    private final OrdersProperties props;
    private final FixMessageParser parser;
    private final BatchStats batchStats;
    private final AtomicLong skipped = new AtomicLong();

    public OrdersRoute(OrdersProperties props, FixMessageParser parser, BatchStats batchStats) {
        this.props = props;
        this.parser = parser;
        this.batchStats = batchStats;
    }

    @Override
    public void configure() {
        ExecutorService batchExecutor = getCamelContext().getExecutorServiceManager()
                .newSingleThreadExecutor(this, "uc1-batch");

        // Poison-message policy: log, acknowledge (so the partition moves on) and stop routing the record.
        onException(FixParseException.class)
                .handled(true)
                .process(this::skipInvalidOrder);

        from(kafkaUri())
                .routeId("uc1-orders")
                .process(exchange -> exchange.getMessage().setBody(
                        parser.parseNewOrderSingle(exchange.getMessage().getBody(String.class))))
                .aggregate(constant(true), new GroupedExchangeAggregationStrategy())
                        .completionSize(props.batchSize())
                        .completionTimeout(props.batchTimeout().toMillis())
                        .completionTimeoutCheckerInterval(100)
                        .parallelProcessing()
                        .executorService(batchExecutor)
                        // body: List<Exchange> -> List<Map> of SQL parameters, records kept in a property
                        .process(this::toSqlParameters)
                        .to("sql:" + OrderSql.INSERT_CAMEL.replaceAll("\\s+", " ") + "?batch=true")
                        .process(this::acknowledgeBatch)
                .end();
    }

    private String kafkaUri() {
        return "kafka:" + props.topic()
                + "?groupId=" + props.groupId()
                + "&consumersCount=" + props.concurrency()
                + "&autoOffsetReset=earliest"
                + "&autoCommitEnable=false"
                + "&allowManualCommit=true"
                + "&maxPollRecords=" + props.batchSize()
                + "&pollTimeoutMs=250";
    }

    @SuppressWarnings("unchecked")
    private void toSqlParameters(Exchange batch) {
        List<Exchange> records = batch.getMessage().getBody(List.class);
        batch.setProperty(BATCH_RECORDS, records);
        List<Map<String, Object>> rows = records.stream()
                .map(record -> OrderSql.parameters(record.getMessage().getBody(NewOrder.class)))
                .toList();
        batch.getMessage().setBody(rows);
    }

    @SuppressWarnings("unchecked")
    private void acknowledgeBatch(Exchange batch) {
        List<Exchange> records = batch.getProperty(BATCH_RECORDS, List.class);
        KafkaManualCommits.commitAll(records);
        batchStats.recordBatch(records.size());
        log.info("Inserted and acknowledged a batch of {} orders on {}", records.size(),
                Thread.currentThread().getName());
    }

    private void skipInvalidOrder(Exchange exchange) {
        Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        log.warn("Skipping invalid order from {}-{}@{}: {}",
                exchange.getMessage().getHeader(KafkaConstants.TOPIC),
                exchange.getMessage().getHeader(KafkaConstants.PARTITION),
                exchange.getMessage().getHeader(KafkaConstants.OFFSET),
                cause == null ? "unknown" : cause.getMessage());
        KafkaManualCommits.commit(exchange);
        skipped.incrementAndGet();
    }

    public long skipped() {
        return skipped.get();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OrdersProperties.class)
    static class Beans {

        @Bean
        FixMessageParser fixMessageParser() {
            return new FixMessageParser();
        }

        @Bean
        BatchStats batchStats() {
            return new BatchStats();
        }
    }
}
