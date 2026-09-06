package com.fixflow.usecase1.camel;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic Kafka topic that receives one record per support alert (rows skipped by the insert fallback)
 */
@ConfigurationProperties("fixflow.alerts")
public record AlertProperties(@DefaultValue("support-alerts") String topic) {
}
