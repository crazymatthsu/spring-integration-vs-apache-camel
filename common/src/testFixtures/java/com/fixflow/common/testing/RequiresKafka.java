package com.fixflow.common.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/** Skips the test (with a clear reason) when the Kafka broker from {@code podman compose} is not reachable. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(KafkaAvailableCondition.class)
public @interface RequiresKafka {
}
