package com.fixflow.common.alerts;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import com.fixflow.common.fix.FixMessageFactory;
import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import com.fixflow.common.orders.OrderSql;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import static org.assertj.core.api.Assertions.assertThat;

class SupportAlerterTest {

    private final NewOrder order = new FixMessageParser().parseNewOrderSingle(
            new FixMessageFactory("CLIENT1", "BROKER").newOrderSingle("ORD-1", "AAPL", '1', BigDecimal.TEN, null));

    @Test
    void failureCarriesTheOrderIdentityAndTheDeepestCause() {
        DuplicateKeyException error = new DuplicateKeyException("insert failed",
                new java.sql.SQLException("[SQLITE_CONSTRAINT_UNIQUE] A UNIQUE constraint failed\nsecond line"));

        InsertFailure fromOrder = InsertFailure.of(order, error);
        InsertFailure fromParameters = InsertFailure.of(OrderSql.parameters(order), error);

        assertThat(fromOrder).isEqualTo(fromParameters);
        assertThat(fromOrder.clOrdId()).isEqualTo("ORD-1");
        assertThat(fromOrder.senderCompId()).isEqualTo("CLIENT1");
        assertThat(fromOrder.reason()).isEqualTo("SQLException: [SQLITE_CONSTRAINT_UNIQUE] A UNIQUE constraint failed");
    }

    @Test
    void loggingAlerterKeepsRecentAlerts() {
        LoggingSupportAlerter alerter = new LoggingSupportAlerter();
        SupportAlert alert = SupportAlert.now("test-app", "1 of 3 orders could not be inserted",
                List.of(InsertFailure.of(order, new DuplicateKeyException("duplicate"))));

        alerter.raise(alert);

        assertThat(alerter.recent()).containsExactly(alert);
        assertThat(alert.toLogLine())
                .startsWith("SUPPORT-ALERT source=test-app")
                .contains("summary=\"1 of 3 orders could not be inserted\"")
                .contains("ORD-1/CLIENT1");
    }

    @Test
    void compositeFansOutAndSurvivesAFailingAlerter() {
        LoggingSupportAlerter first = new LoggingSupportAlerter();
        LoggingSupportAlerter last = new LoggingSupportAlerter();
        SupportAlerter broken = a -> {
            throw new IllegalStateException("pager is down");
        };
        SupportAlert alert = SupportAlert.now("test-app", "summary", List.of());

        new CompositeSupportAlerter(List.of(first, broken, last)).raise(alert);

        assertThat(first.recent()).containsExactly(alert);
        assertThat(last.recent()).containsExactly(alert);
    }

    @Test
    void failureFromParametersToleratesMissingKeys() {
        InsertFailure failure = InsertFailure.of(Map.of(), new RuntimeException("boom"));

        assertThat(failure.clOrdId()).isEqualTo("null");
        assertThat(failure.reason()).isEqualTo("RuntimeException: boom");
    }
}
