package com.fixflow.common.orders;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import com.fixflow.common.fix.FixMessageFactory;
import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.dao.DataAccessException;
import org.sqlite.SQLiteDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderRepositoryTest {

    private final FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");
    private final FixMessageParser parser = new FixMessageParser();

    private OrderRepository repository;

    @BeforeEach
    void setUp(@TempDir Path dir) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:" + dir.resolve("orders.db"));
        OrderSchema.apply(dataSource);
        repository = new OrderRepository(dataSource);
    }

    @Test
    void insertsABatch() {
        List<NewOrder> orders = List.of(order("A"), order("B"), order("C"));

        assertThat(repository.insertBatch(orders)).containsExactly(1, 1, 1);
        assertThat(repository.count()).isEqualTo(3);
        assertThat(repository.findAllClOrdIds()).containsExactly("A", "B", "C");
    }

    @Test
    void aDuplicateIsARowLevelFailure() {
        repository.insertBatch(List.of(order("A"), order("B")));

        // the same (sender, clOrdId) again, e.g. a redelivered or replayed order
        assertThatThrownBy(() -> repository.insertBatch(List.of(order("A"))))
                .isInstanceOf(DataAccessException.class)
                .satisfies(error -> assertThat(InsertFailurePolicy.isRowLevel(error)).isTrue());

        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void aDuplicateInsideABatchFailsTheBatch() {
        repository.insertBatch(List.of(order("A")));

        assertThatThrownBy(() -> repository.insertBatch(List.of(order("B"), order("A"), order("C"))))
                .isInstanceOf(DataAccessException.class)
                .satisfies(error -> assertThat(InsertFailurePolicy.isRowLevel(error)).isTrue());
    }

    @Test
    void policyTreatsDatabaseProblemsAsNotRowLevel() {
        assertThat(InsertFailurePolicy.isRowLevel(
                new org.springframework.dao.DataAccessResourceFailureException("connection refused"))).isFalse();
        assertThat(InsertFailurePolicy.isRowLevel(
                new org.springframework.dao.QueryTimeoutException("timed out"))).isFalse();
        assertThat(InsertFailurePolicy.isRowLevel(
                new org.sqlite.SQLiteException("database is locked", org.sqlite.SQLiteErrorCode.SQLITE_BUSY))).isFalse();
        assertThat(InsertFailurePolicy.isRowLevel(
                new org.sqlite.SQLiteException("UNIQUE constraint failed",
                        org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE))).isTrue();
        assertThat(InsertFailurePolicy.isRowLevel(
                new org.springframework.dao.DuplicateKeyException("duplicate"))).isTrue();
    }

    private NewOrder order(String clOrdId) {
        return parser.parseNewOrderSingle(
                factory.newOrderSingle(clOrdId, "AAPL", '1', new BigDecimal("100"), new BigDecimal("10.5")));
    }
}
