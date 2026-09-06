package com.fixflow.usecase1.si;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.fixflow.common.alerts.InsertFailure;
import com.fixflow.common.alerts.SupportAlert;
import com.fixflow.common.alerts.SupportAlerter;
import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import com.fixflow.common.orders.BatchStats;
import com.fixflow.common.orders.InsertFailurePolicy;
import com.fixflow.common.orders.OrderSql;
import com.fixflow.support.kafka.KafkaAcks;
import com.fixflow.support.kafka.ManualAckContainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice;
import org.springframework.integration.jdbc.outbound.JdbcMessageHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * The Spring Integration flow of use case 1:
 * <pre>
 *  Kafka (manual ack) -> ExecutorChannel -> handle (FIX -> NewOrder) -> aggregate (size | timeout)
 *      -> pub/sub: [1] JdbcMessageHandler batch INSERT (one transaction)   [2] acknowledge every record of the batch
 *
 *  batch INSERT failed on a row -> batchInsertFallback: split -> insert one by one -> aggregate -> alert support
 * </pre>
 * The Kafka consumer thread only hands the record over to the executor; parsing, batching, the JDBC batch insert,
 * the fallback and the acknowledgments all happen on the {@code uc1-batch-} thread (or the scheduler thread for a
 * timeout). The fallback runs inside the trapped failure of the batch insert, so the acknowledgment step only runs
 * after every row has been tried; a database-level failure aborts instead and nothing is acknowledged.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OrdersProperties.class)
public class OrdersFlowConfiguration {

    /** Header on the aggregated message: the {@link Acknowledgment} of every record that went into the batch. */
    static final String BATCH_ACKS = "fixflow_batchAcknowledgments";

    /** Header on the fallback messages: the exception that failed the batch insert. */
    static final String BATCH_FAILURE = "fixflow_batchFailure";

    /** Channel receiving the records that cannot be parsed. */
    static final String INVALID_ORDERS_CHANNEL = "invalidOrders";

    /** Channel receiving a batch whose JDBC batch insert failed on a row. */
    static final String BATCH_INSERT_FALLBACK_CHANNEL = "batchInsertFallback";

    private static final Logger log = LoggerFactory.getLogger(OrdersFlowConfiguration.class);

    @Bean
    FixMessageParser fixMessageParser() {
        return new FixMessageParser();
    }

    @Bean
    BatchStats batchStats() {
        return new BatchStats();
    }

    @Bean
    ConcurrentMessageListenerContainer<String, String> ordersListenerContainer(
            ConsumerFactory<String, String> consumerFactory, OrdersProperties props) {
        return ManualAckContainers.manualAsyncAck(consumerFactory, props.topic(), props.groupId(), props.concurrency());
    }

    /** Single thread: batches are inserted (and acknowledged) in arrival order. */
    @Bean
    TaskExecutor ordersBatchExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setThreadNamePrefix("uc1-batch-");
        return executor;
    }

    /** Spring Integration's JDBC outbound adapter performs a JDBC batch update when the payload is an Iterable. */
    @Bean
    JdbcMessageHandler ordersBatchInsert(DataSource dataSource) {
        JdbcMessageHandler handler = new JdbcMessageHandler(dataSource, OrderSql.INSERT);
        handler.setUsePayloadAsParameterSource(true);
        handler.setSqlParameterSourceFactory(order -> new MapSqlParameterSource(OrderSql.parameters((NewOrder) order)));
        return handler;
    }

    /** Traps parse failures of a single record and forwards them (with the original message) to the invalid channel. */
    @Bean
    ExpressionEvaluatingRequestHandlerAdvice invalidOrderAdvice() {
        ExpressionEvaluatingRequestHandlerAdvice advice = new ExpressionEvaluatingRequestHandlerAdvice();
        advice.setFailureChannelName(INVALID_ORDERS_CHANNEL);
        advice.setTrapException(true);
        return advice;
    }

    /** Traps a failed batch insert (its transaction already rolled back) and hands the batch to the fallback flow. */
    @Bean
    ExpressionEvaluatingRequestHandlerAdvice batchInsertFallbackAdvice() {
        ExpressionEvaluatingRequestHandlerAdvice advice = new ExpressionEvaluatingRequestHandlerAdvice();
        advice.setFailureChannelName(BATCH_INSERT_FALLBACK_CHANNEL);
        advice.setTrapException(true);
        return advice;
    }

    @Bean
    IntegrationFlow ordersFlow(ConcurrentMessageListenerContainer<String, String> ordersListenerContainer,
                               TaskExecutor ordersBatchExecutor,
                               FixMessageParser parser,
                               ExpressionEvaluatingRequestHandlerAdvice invalidOrderAdvice,
                               JdbcMessageHandler ordersBatchInsert,
                               ExpressionEvaluatingRequestHandlerAdvice batchInsertFallbackAdvice,
                               BatchStats batchStats,
                               OrdersProperties props) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(ordersListenerContainer,
                        KafkaMessageDrivenChannelAdapter.ListenerMode.record))
                // asynchronous hand-off: the Kafka consumer thread returns here
                .channel(MessageChannels.executor("ordersChannel", ordersBatchExecutor))
                // a service activator rather than a transformer: a trapped parse failure yields no reply,
                // which ends the flow for that record (a transformer must always reply)
                .handle(String.class, (fix, headers) -> parser.parseNewOrderSingle(fix),
                        e -> e.advice(invalidOrderAdvice).requiresReply(false))
                .aggregate(aggregator -> aggregator
                        .correlationStrategy(message -> "orders")
                        .releaseStrategy(group -> group.size() >= props.batchSize())
                        .groupTimeout(props.batchTimeout().toMillis())
                        .sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .outputProcessor(OrdersFlowConfiguration::toBatch))
                // subscribers run sequentially; the acknowledgment step is skipped when the insert throws
                .publishSubscribeChannel(pubSub -> pubSub
                        // one transaction per batch: a failed batch leaves nothing behind for the fallback;
                        // the fallback advice is outermost, so it sees the failure after the rollback
                        .subscribe(flow -> flow.handle(ordersBatchInsert,
                                e -> e.advice(batchInsertFallbackAdvice).transactional()))
                        .subscribe(flow -> flow.handle(message -> acknowledge(message, batchStats))))
                .get();
    }

    /**
     * Fallback after a batch insert failed on a row: insert the rows one by one, skip the rows that are at fault,
     * report them to support. Runs synchronously on the batch thread, inside the trapped failure.
     */
    @Bean
    IntegrationFlow batchInsertFallbackFlow(NamedParameterJdbcTemplate jdbcTemplate,
                                            SupportAlerter supportAlerter,
                                            BatchStats batchStats,
                                            @Value("${spring.application.name}") String applicationName) {
        return IntegrationFlow.from(BATCH_INSERT_FALLBACK_CHANNEL)
                // the ErrorMessage carries the failed batch; abort (rethrow) unless the failure is row-level
                .transform(MessagingException.class, OrdersFlowConfiguration::failedBatch)
                .split()
                .handle(NewOrder.class, (order, headers) -> insertOne(jdbcTemplate, order))
                .aggregate(aggregator -> aggregator.expireGroupsUponCompletion(true))
                .handle(List.class, (results, headers) -> {
                    report(results, headers, supportAlerter, batchStats, applicationName);
                    return null;
                })
                .get();
    }

    @Bean
    IntegrationFlow invalidOrdersFlow(InvalidOrderHandler invalidOrderHandler) {
        return IntegrationFlow.from(INVALID_ORDERS_CHANNEL)
                .handle(invalidOrderHandler)
                .get();
    }

    @Bean
    InvalidOrderHandler invalidOrderHandler() {
        return new InvalidOrderHandler();
    }

    /** Output of the aggregator: the orders of the group, plus the acknowledgment of every record. */
    private static Message<List<NewOrder>> toBatch(MessageGroup group) {
        List<NewOrder> orders = new ArrayList<>(group.size());
        List<Acknowledgment> acks = new ArrayList<>(group.size());
        for (Message<?> message : group.getMessages()) {
            orders.add((NewOrder) message.getPayload());
            acks.add(KafkaAcks.from(message.getHeaders()));
        }
        return MessageBuilder.withPayload(orders).setHeader(BATCH_ACKS, acks).build();
    }

    @SuppressWarnings("unchecked")
    private static void acknowledge(Message<?> batch, BatchStats batchStats) {
        List<Acknowledgment> acks = (List<Acknowledgment>) batch.getHeaders().get(BATCH_ACKS);
        acks.forEach(Acknowledgment::acknowledge);
        batchStats.recordBatch(acks.size());
        log.info("Inserted and acknowledged a batch of {} orders on {}", acks.size(), Thread.currentThread().getName());
    }

    /** The orders of the failed batch, or an exception (aborting the batch) when the database itself is in trouble. */
    @SuppressWarnings("unchecked")
    private static Message<List<NewOrder>> failedBatch(MessagingException failure) {
        Throwable cause = failure.getCause() == null ? failure : failure.getCause();
        if (!InsertFailurePolicy.isRowLevel(cause)) {
            log.error("Batch insert failed because of the database, not a row; nothing acknowledged, "
                    + "the batch will be redelivered: {}", InsertFailure.reasonOf(cause));
            throw failure;
        }
        List<NewOrder> orders = (List<NewOrder>) failure.getFailedMessage().getPayload();
        log.warn("Batch insert of {} orders failed ({}); inserting them one by one",
                orders.size(), InsertFailure.reasonOf(cause));
        return MessageBuilder.withPayload(orders).setHeader(BATCH_FAILURE, cause).build();
    }

    private static RowInsert insertOne(NamedParameterJdbcTemplate jdbcTemplate, NewOrder order) {
        try {
            jdbcTemplate.update(OrderSql.INSERT, new MapSqlParameterSource(OrderSql.parameters(order)));
            return RowInsert.inserted(order);
        }
        catch (DataAccessException e) {
            if (!InsertFailurePolicy.isRowLevel(e)) {
                throw e; // the database is in trouble: abort the fallback, nothing is acknowledged
            }
            log.error("Skipping order {} that could not be inserted: {}", order, InsertFailure.reasonOf(e));
            return RowInsert.failed(order, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void report(List<?> results, MessageHeaders headers, SupportAlerter supportAlerter,
                               BatchStats batchStats, String applicationName) {
        List<RowInsert> rows = (List<RowInsert>) results;
        List<InsertFailure> failures = rows.stream().filter(RowInsert::failed).map(RowInsert::failure).toList();
        Throwable batchFailure = (Throwable) headers.get(BATCH_FAILURE);
        batchStats.recordFallback(failures.size());
        if (failures.isEmpty()) {
            log.info("All {} orders inserted one by one after the batch insert failed", rows.size());
            return;
        }
        supportAlerter.raise(SupportAlert.now(applicationName,
                failures.size() + " of " + rows.size() + " orders could not be inserted after the batch insert failed ("
                        + InsertFailure.reasonOf(batchFailure) + ")",
                failures));
    }

    /** Outcome of one row of the fallback. */
    record RowInsert(NewOrder order, InsertFailure failure) {

        static RowInsert inserted(NewOrder order) {
            return new RowInsert(order, null);
        }

        static RowInsert failed(NewOrder order, Throwable error) {
            return new RowInsert(order, InsertFailure.of(order, error));
        }

        boolean failed() {
            return failure != null;
        }
    }
}
