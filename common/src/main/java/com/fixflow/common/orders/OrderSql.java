package com.fixflow.common.orders;

import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fixflow.common.fix.NewOrder;

/** SQL shared by both frameworks so that the persisted result is identical. */
public final class OrderSql {

    /** Classpath location of the DDL, applied by Spring Boot's {@code spring.sql.init} in every demo. */
    public static final String SCHEMA_RESOURCE = "db/orders-schema.sql";

    /**
     * Insert statement with Spring-style named parameters ({@code :name}).
     * A duplicate {@code (sender_comp_id, cl_ord_id)} violates the unique index and fails the row, which the demos
     * treat as a row-level failure: the batch falls back to one-by-one inserts and the duplicate is reported.
     */
    public static final String INSERT = """
            INSERT INTO orders (cl_ord_id, symbol, side, order_qty, ord_type, price, transact_time,
                                sender_comp_id, target_comp_id, msg_seq_num, raw_message)
            VALUES (:clOrdId, :symbol, :side, :orderQty, :ordType, :price, :transactTime,
                    :senderCompId, :targetCompId, :msgSeqNum, :rawMessage)""";

    /** The same statement with Camel SQL component named parameters ({@code :#name}). */
    public static final String INSERT_CAMEL = INSERT.replace(":", ":#");

    private OrderSql() {
    }

    /** Named parameters for {@link #INSERT} / {@link #INSERT_CAMEL}. */
    public static Map<String, Object> parameters(NewOrder order) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("clOrdId", order.clOrdId());
        params.put("symbol", order.symbol());
        params.put("side", String.valueOf(order.side()));
        params.put("orderQty", order.orderQty().doubleValue());
        params.put("ordType", String.valueOf(order.ordType()));
        params.put("price", order.price() == null ? null : order.price().doubleValue());
        params.put("transactTime", DateTimeFormatter.ISO_INSTANT.format(order.transactTime()));
        params.put("senderCompId", order.senderCompId());
        params.put("targetCompId", order.targetCompId());
        params.put("msgSeqNum", order.msgSeqNum());
        params.put("rawMessage", order.rawMessage());
        return params;
    }
}
