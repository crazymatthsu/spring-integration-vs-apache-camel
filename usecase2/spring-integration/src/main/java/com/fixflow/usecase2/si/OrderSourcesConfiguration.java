package com.fixflow.usecase2.si;

import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.orders.OrderProcessor;
import com.fixflow.support.kafka.ManualAckContainers;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * The Spring Integration flows of use case 2:
 * <pre>
 *  Kafka algo (manual ack) --\
 *                             --> PublishSubscribeChannel (executor) --> OrderHandler --> ack to the source topic
 *  Kafka dma  (manual ack) --/
 * </pre>
 * The two Kafka consumer threads only publish to the in-memory channel; the handler runs on the executor threads.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OrderSourcesProperties.class)
public class OrderSourcesConfiguration {

    static final String ORDERS_CHANNEL = "orders";

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

    @Bean
    ConcurrentMessageListenerContainer<String, String> algoListenerContainer(
            ConsumerFactory<String, String> consumerFactory, OrderSourcesProperties props) {
        return ManualAckContainers.manualAsyncAck(consumerFactory, props.algoTopic(), props.groupId(), props.concurrency());
    }

    @Bean
    ConcurrentMessageListenerContainer<String, String> dmaListenerContainer(
            ConsumerFactory<String, String> consumerFactory, OrderSourcesProperties props) {
        return ManualAckContainers.manualAsyncAck(consumerFactory, props.dmaTopic(), props.groupId(), props.concurrency());
    }

    @Bean
    TaskExecutor orderHandlerExecutor(OrderSourcesProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.handlerThreads());
        executor.setMaxPoolSize(props.handlerThreads());
        executor.setThreadNamePrefix("uc2-handler-");
        return executor;
    }

    /** In-memory publish/subscribe channel; subscribers are invoked on the executor, not on the consumer threads. */
    @Bean(ORDERS_CHANNEL)
    MessageChannel ordersChannel(TaskExecutor orderHandlerExecutor) {
        return new PublishSubscribeChannel(orderHandlerExecutor);
    }

    @Bean
    IntegrationFlow algoInboundFlow(ConcurrentMessageListenerContainer<String, String> algoListenerContainer) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(algoListenerContainer,
                        KafkaMessageDrivenChannelAdapter.ListenerMode.record))
                .channel(ORDERS_CHANNEL)
                .get();
    }

    @Bean
    IntegrationFlow dmaInboundFlow(ConcurrentMessageListenerContainer<String, String> dmaListenerContainer) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(dmaListenerContainer,
                        KafkaMessageDrivenChannelAdapter.ListenerMode.record))
                .channel(ORDERS_CHANNEL)
                .get();
    }

    @Bean
    IntegrationFlow orderHandlerFlow(OrderHandler orderHandler) {
        return IntegrationFlow.from(ORDERS_CHANNEL)
                .handle(orderHandler)
                .get();
    }
}
