package com.fixflow.usecase1.si;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import com.fixflow.common.orders.BatchStats;
import com.fixflow.common.orders.OrderSql;
import com.fixflow.support.kafka.KafkaAcks;
import com.fixflow.support.kafka.ManualAckContainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice;
import org.springframework.integration.jdbc.outbound.JdbcMessageHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * The Spring Integration flow of use case 1:
 * <pre>
 *  Kafka (manual ack) -> ExecutorChannel -> transform (FIX -> NewOrder) -> aggregate (size | timeout)
 *      -> pub/sub: [1] JdbcMessageHandler batch INSERT   [2] acknowledge every record of the batch
 * </pre>
 * The Kafka consumer thread only hands the record over to the executor; parsing, batching, the JDBC batch insert
 * and the acknowledgments all happen on the {@code uc1-batch-} thread (or the scheduler thread for a timeout).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OrdersProperties.class)
public class OrdersFlowConfiguration {

    /** Header on the aggregated message: the {@link Acknowledgment} of every record that went into the batch. */
    static final String BATCH_ACKS = "fixflow_batchAcknowledgments";

    /** Channel receiving the records that cannot be parsed. */
    static final String INVALID_ORDERS_CHANNEL = "invalidOrders";

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

    @Bean
    IntegrationFlow ordersFlow(ConcurrentMessageListenerContainer<String, String> ordersListenerContainer,
                               TaskExecutor ordersBatchExecutor,
                               FixMessageParser parser,
                               ExpressionEvaluatingRequestHandlerAdvice invalidOrderAdvice,
                               JdbcMessageHandler ordersBatchInsert,
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
                        .subscribe(flow -> flow.handle(ordersBatchInsert))
                        .subscribe(flow -> flow.handle(message -> acknowledge(message, batchStats))))
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
}
