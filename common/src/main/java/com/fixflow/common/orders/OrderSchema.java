package com.fixflow.common.orders;

import javax.sql.DataSource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/** Applies {@link OrderSql#SCHEMA_RESOURCE} outside Spring Boot (tests, tools). */
public final class OrderSchema {

    private OrderSchema() {
    }

    public static void apply(DataSource dataSource) {
        new ResourceDatabasePopulator(new ClassPathResource(OrderSql.SCHEMA_RESOURCE)).execute(dataSource);
    }
}
