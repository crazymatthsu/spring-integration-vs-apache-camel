package com.fixflow.usecase1.si;

import java.util.ArrayList;
import java.util.List;

import com.fixflow.common.alerts.CompositeSupportAlerter;
import com.fixflow.common.alerts.KafkaSupportAlerter;
import com.fixflow.common.alerts.LoggingSupportAlerter;
import com.fixflow.common.alerts.SupportAlerter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * How the support team is notified about rows skipped by the insert fallback: always an ERROR log line with the
 * {@code SUPPORT_ALERT} marker, plus a record on the {@code fixflow.alerts.topic} Kafka topic when one is configured.
 * Add e-mail, PagerDuty or Slack by contributing another {@link SupportAlerter} bean.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AlertProperties.class)
public class SupportAlertConfiguration {

    @Bean
    LoggingSupportAlerter loggingSupportAlerter() {
        return new LoggingSupportAlerter();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "fixflow.alerts", name = "topic")
    KafkaSupportAlerter kafkaSupportAlerter(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                            AlertProperties props) {
        return new KafkaSupportAlerter(bootstrapServers, props.topic());
    }

    @Bean
    @Primary
    SupportAlerter supportAlerter(LoggingSupportAlerter loggingSupportAlerter,
                                  ObjectProvider<KafkaSupportAlerter> kafkaSupportAlerter) {
        List<SupportAlerter> alerters = new ArrayList<>();
        alerters.add(loggingSupportAlerter);
        kafkaSupportAlerter.ifAvailable(alerters::add);
        return new CompositeSupportAlerter(alerters);
    }
}
