package com.fixflow.usecase1.camel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.fixflow.common.alerts.InsertFailure;
import com.fixflow.common.alerts.SupportAlert;
import com.fixflow.common.alerts.SupportAlerter;
import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.FixParseException;
import com.fixflow.common.fix.NewOrder;
import com.fixflow.common.orders.BatchStats;
import com.fixflow.common.orders.InsertFailurePolicy;
import com.fixflow.common.orders.OrderSql;
import com.fixflow.support.camel.KafkaManualCommits;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.processor.aggregate.GroupedExchangeAggregationStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * The Camel routes of use case 1:
 * <pre>
 *  uc1-orders:             kafka (manual commit) -> process (FIX -> NewOrder)
 *                              -> aggregate (completionSize | completionTimeout, own thread)
 *                                  -> direct:insertBatch -> acknowledge every record of the batch
 *  uc1-insert-batch:       doTry sql (one batch INSERT, one transaction) doCatch -> direct:insertOneByOne
 *  uc1-insert-one-by-one:  split rows -> direct:insertRow -> alert support about the skipped rows
 *  uc1-insert-row:         doTry sql (one INSERT) doCatch -> skip the row if it is at fault
 * </pre>
 * The Kafka consumer thread parses and drops the record into the aggregator; the completed batch, the fallback and
 * the acknowledgments run on the {@code uc1-batch} thread ({@code direct:} routes run on the caller's thread).
 * Acknowledgments issued there are parked and committed by the consumer thread before its next poll, contiguous
 * offsets only (see {@code camel-kafka-support}). A database-level failure aborts the batch instead, so nothing is
 * acknowledged and Kafka redelivers it.
 */
@Component
public class OrdersRoute extends RouteBuilder {

    /** Exchange property holding the original record exchanges of the batch while the body is the SQL parameter list. */
    static final String BATCH_RECORDS = "fixflowBatchRecords";

    /** Exchange property holding the exception that failed the batch insert. */
    static final String BATCH_FAILURE = "fixflowBatchFailure";

    /** Exchange property collecting the rows skipped by the fallback; the list is shared with the split sub-exchanges. */
    static final String ROW_FAILURES = "fixflowRowFailures";

    private static final String INSERT_SQL = OrderSql.INSERT_CAMEL.replaceAll("\\s+", " ");

    /** One JDBC batch in one transaction: a failed batch is rolled back before the fallback runs. */
    static final String BATCH_INSERT = "sql:" + INSERT_SQL + "?batch=true&batchAutoCommitDisabled=true";

    /** One row per call, used by the fallback. */
    static final String SINGLE_INSERT = "sql:" + INSERT_SQL;

    private final OrdersProperties props;
    private final FixMessageParser parser;
    private final BatchStats batchStats;
    private final SupportAlerter supportAlerter;
    private final String applicationName;
    private final AtomicLong skipped = new AtomicLong();

    public OrdersRoute(OrdersProperties props, FixMessageParser parser, BatchStats batchStats,
                       SupportAlerter supportAlerter, @Value("${spring.application.name}") String applicationName) {
        this.props = props;
        this.parser = parser;
        this.batchStats = batchStats;
        this.supportAlerter = supportAlerter;
        this.applicationName = applicationName;
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
                        // throws (so nothing below runs) only when the database itself is in trouble
                        .to("direct:insertBatch")
                        .process(this::acknowledgeBatch)
                .end();

        from("direct:insertBatch")
                .routeId("uc1-insert-batch")
                .doTry()
                        .to(BATCH_INSERT)
                .doCatch(DataAccessException.class)
                        .to("direct:insertOneByOne")
                .end();

        from("direct:insertOneByOne")
                .routeId("uc1-insert-one-by-one")
                // rethrows database-level failures: the batch is not acknowledged and comes back
                .process(this::startFallback)
                .split(body()).stopOnException()
                        .to("direct:insertRow")
                .end()
                .process(this::reportFallback);

        from("direct:insertRow")
                .routeId("uc1-insert-row")
                .doTry()
                        .to(SINGLE_INSERT)
                .doCatch(DataAccessException.class)
                        .process(this::skipFailedRow)
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

    /** Decides whether the failed batch may fall back to one-by-one inserts, and prepares the fallback. */
    private void startFallback(Exchange batch) throws Exception {
        Exception cause = batch.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        if (!InsertFailurePolicy.isRowLevel(cause)) {
            log.error("Batch insert failed because of the database, not a row; nothing acknowledged, "
                    + "the batch will be redelivered: {}", InsertFailure.reasonOf(cause));
            throw cause;
        }
        int rows = batch.getMessage().getBody(List.class).size();
        log.warn("Batch insert of {} orders failed ({}); inserting them one by one", rows, InsertFailure.reasonOf(cause));
        batch.setProperty(BATCH_FAILURE, cause);
        batch.setProperty(ROW_FAILURES, new ArrayList<InsertFailure>());
    }

    /** Runs for every row of the fallback that failed: skip it if it is at fault, abort if the database is. */
    @SuppressWarnings("unchecked")
    private void skipFailedRow(Exchange row) throws Exception {
        Exception cause = row.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        if (!InsertFailurePolicy.isRowLevel(cause)) {
            throw cause;
        }
        InsertFailure failure = InsertFailure.of(row.getMessage().getBody(Map.class), cause);
        log.error("Skipping order {} that could not be inserted: {}", failure.clOrdId(), failure.reason());
        row.getProperty(ROW_FAILURES, List.class).add(failure);
    }

    @SuppressWarnings("unchecked")
    private void reportFallback(Exchange batch) {
        List<InsertFailure> failures = batch.getProperty(ROW_FAILURES, List.class);
        Exception cause = batch.getProperty(BATCH_FAILURE, Exception.class);
        int rows = batch.getMessage().getBody(List.class).size();
        batchStats.recordFallback(failures.size());
        if (failures.isEmpty()) {
            log.info("All {} orders inserted one by one after the batch insert failed", rows);
            return;
        }
        supportAlerter.raise(SupportAlert.now(applicationName,
                failures.size() + " of " + rows + " orders could not be inserted after the batch insert failed ("
                        + InsertFailure.reasonOf(cause) + ")",
                failures));
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
