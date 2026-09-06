package com.fixflow.common.orders;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import com.fixflow.common.fix.FixMessageFactory;
import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import static org.assertj.core.api.Assertions.assertThat;

class OrderRepositoryTest {

    private final FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");
    private final FixMessageParser parser = new FixMessageParser();

    @Test
    void batchInsertIsIdempotent(@TempDir Path dir) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:" + dir.resolve("orders.db"));
        OrderSchema.apply(dataSource);
        OrderRepository repository = new OrderRepository(dataSource);

        List<NewOrder> orders = List.of(order("A"), order("B"), order("C"));

        assertThat(repository.insertBatch(orders)).containsExactly(1, 1, 1);
        assertThat(repository.count()).isEqualTo(3);
        assertThat(repository.findAllClOrdIds()).containsExactly("A", "B", "C");

        // a redelivered batch (at-least-once) must not create duplicates
        assertThat(repository.insertBatch(orders)).containsExactly(0, 0, 0);
        assertThat(repository.count()).isEqualTo(3);
    }

    private NewOrder order(String clOrdId) {
        return parser.parseNewOrderSingle(
                factory.newOrderSingle(clOrdId, "AAPL", '1', new BigDecimal("100"), new BigDecimal("10.5")));
    }
}
