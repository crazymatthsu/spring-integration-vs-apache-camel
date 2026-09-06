package com.fixflow.common.orders;

import java.util.List;

import javax.sql.DataSource;

import com.fixflow.common.fix.NewOrder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Plain Spring JDBC access to the {@code orders} table.
 * The demos persist through their framework's own JDBC component; this class exists for the tests and tools.
 */
public class OrderRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OrderRepository(DataSource dataSource) {
        this.jdbc = new NamedParameterJdbcTemplate(dataSource);
    }

    /** @return the update count per order (0 when the order was already stored). */
    public int[] insertBatch(List<NewOrder> orders) {
        SqlParameterSource[] batch = orders.stream()
                .map(order -> new MapSqlParameterSource(OrderSql.parameters(order)))
                .toArray(SqlParameterSource[]::new);
        return jdbc.batchUpdate(OrderSql.INSERT, batch);
    }

    public long count() {
        Long count = jdbc.getJdbcOperations().queryForObject("SELECT COUNT(*) FROM orders", Long.class);
        return count == null ? 0 : count;
    }

    public List<String> findAllClOrdIds() {
        return jdbc.getJdbcOperations().queryForList("SELECT cl_ord_id FROM orders ORDER BY id", String.class);
    }
}
