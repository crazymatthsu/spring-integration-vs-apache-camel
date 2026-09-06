package com.fixflow.usecase2.si;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param algoTopic      Kafka topic with algo FIX 4.2 NewOrderSingle messages
 * @param dmaTopic       Kafka topic with DMA FIX 4.2 NewOrderSingle messages
 * @param groupId        consumer group of this application (used for both topics)
 * @param handlerThreads worker threads invoking the handler
 * @param concurrency    Kafka consumer threads per topic
 */
@ConfigurationProperties("fixflow.sources")
public record OrderSourcesProperties(
        @DefaultValue("algo") String algoTopic,
        @DefaultValue("dma") String dmaTopic,
        @DefaultValue("uc2-spring-integration") String groupId,
        @DefaultValue("4") int handlerThreads,
        @DefaultValue("1") int concurrency) {
}
