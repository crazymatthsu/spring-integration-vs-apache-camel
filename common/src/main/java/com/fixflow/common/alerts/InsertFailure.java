package com.fixflow.common.alerts;

import java.util.Map;
import java.util.Objects;

import com.fixflow.common.fix.NewOrder;

/**
 * One row that could not be inserted during the one-by-one fallback.
 *
 * @param clOrdId      tag 11 of the order
 * @param senderCompId tag 49 of the order
 * @param reason       the most specific cause, as {@code ExceptionClass: message}
 */
public record InsertFailure(String clOrdId, String senderCompId, String reason) {

    private static final int MAX_REASON_LENGTH = 300;

    public InsertFailure {
        Objects.requireNonNull(clOrdId, "clOrdId");
        Objects.requireNonNull(senderCompId, "senderCompId");
        Objects.requireNonNull(reason, "reason");
    }

    public static InsertFailure of(NewOrder order, Throwable error) {
        return new InsertFailure(order.clOrdId(), order.senderCompId(), reasonOf(error));
    }

    /** From the named SQL parameters produced by {@link com.fixflow.common.orders.OrderSql#parameters}. */
    public static InsertFailure of(Map<String, ?> sqlParameters, Throwable error) {
        return new InsertFailure(String.valueOf(sqlParameters.get("clOrdId")),
                String.valueOf(sqlParameters.get("senderCompId")), reasonOf(error));
    }

    /** The class and first message line of the deepest cause, truncated to keep alerts readable. */
    public static String reasonOf(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage() == null ? "" : cause.getMessage().lines().findFirst().orElse("");
        String reason = cause.getClass().getSimpleName() + ": " + message;
        return reason.length() > MAX_REASON_LENGTH ? reason.substring(0, MAX_REASON_LENGTH) + "..." : reason;
    }

    @Override
    public String toString() {
        return clOrdId + "/" + senderCompId + " (" + reason + ")";
    }
}
