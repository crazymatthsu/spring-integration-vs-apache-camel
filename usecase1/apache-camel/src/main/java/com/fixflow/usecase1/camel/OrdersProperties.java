package com.fixflow.usecase1.camel;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic        Kafka topic carrying FIX 4.2 NewOrderSingle messages
 * @param groupId      consumer group of this application
 * @param batchSize    a batch is inserted as soon as it holds this many orders ...
 * @param batchTimeout ... or when the oldest order in it is this old
 * @param concurrency  number of Kafka consumer threads (partitions are spread over them)
 */
@ConfigurationProperties("fixflow.orders")
public record OrdersProperties(
        @DefaultValue("orders") String topic,
        @DefaultValue("uc1-apache-camel") String groupId,
        @DefaultValue("50") int batchSize,
        @DefaultValue("500ms") Duration batchTimeout,
        @DefaultValue("1") int concurrency) {
}
