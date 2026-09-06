package com.fixflow.usecase2.camel;

import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.orders.OrderProcessor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * The Camel routes of use case 2:
 * <pre>
 *  kafka algo (manual commit) --\
 *                                --> seda:orders (concurrent consumers) --> OrderHandler --> commit to the source topic
 *  kafka dma  (manual commit) --/
 * </pre>
 * The two Kafka consumer threads only enqueue to the in-memory seda queue; the handler runs on the seda threads.
 * Commits issued there are parked and applied by the owning consumer thread before its next poll.
 */
@Component
public class OrderSourcesRoutes extends RouteBuilder {

    static final String ORDERS_QUEUE = "seda:orders";

    private final OrderSourcesProperties props;
    private final OrderHandler orderHandler;

    public OrderSourcesRoutes(OrderSourcesProperties props, OrderHandler orderHandler) {
        this.props = props;
        this.orderHandler = orderHandler;
    }

    @Override
    public void configure() {
        from(kafkaUri(props.algoTopic()))
                .routeId("uc2-algo")
                .to(ORDERS_QUEUE + "?blockWhenFull=true");

        from(kafkaUri(props.dmaTopic()))
                .routeId("uc2-dma")
                .to(ORDERS_QUEUE + "?blockWhenFull=true");

        from(ORDERS_QUEUE + "?concurrentConsumers=" + props.handlerThreads())
                .routeId("uc2-handler")
                .process(orderHandler);
    }

    private String kafkaUri(String topic) {
        return "kafka:" + topic
                + "?groupId=" + props.groupId()
                + "&consumersCount=" + props.concurrency()
                + "&autoOffsetReset=earliest"
                + "&autoCommitEnable=false"
                + "&allowManualCommit=true"
                + "&pollTimeoutMs=250";
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OrderSourcesProperties.class)
    static class Beans {

        @Bean
        FixMessageParser fixMessageParser() {
            return new FixMessageParser();
        }

        @Bean
        OrderProcessor orderProcessor(FixMessageParser parser) {
            return new OrderProcessor(parser);
        }

        @Bean
        OrderHandler orderHandler(OrderProcessor orderProcessor) {
            return new OrderHandler(orderProcessor);
        }
    }
}
