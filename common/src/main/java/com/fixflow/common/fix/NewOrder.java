package com.fixflow.common.fix;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * A FIX 4.2 NewOrderSingle (35=D) reduced to the fields the demos persist.
 *
 * @param clOrdId       tag 11
 * @param symbol        tag 55
 * @param side          tag 54 (FIX character value, e.g. '1' = Buy, '2' = Sell)
 * @param orderQty      tag 38
 * @param ordType       tag 40 (FIX character value, e.g. '1' = Market, '2' = Limit)
 * @param price         tag 44, {@code null} for market orders
 * @param transactTime  tag 60 (UTC)
 * @param senderCompId  tag 49
 * @param targetCompId  tag 56
 * @param msgSeqNum     tag 34
 * @param rawMessage    the original FIX string, kept for audit purposes
 */
public record NewOrder(
        String clOrdId,
        String symbol,
        char side,
        BigDecimal orderQty,
        char ordType,
        BigDecimal price,
        Instant transactTime,
        String senderCompId,
        String targetCompId,
        int msgSeqNum,
        String rawMessage) {

    public NewOrder {
        Objects.requireNonNull(clOrdId, "clOrdId");
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(orderQty, "orderQty");
        Objects.requireNonNull(transactTime, "transactTime");
        Objects.requireNonNull(senderCompId, "senderCompId");
        Objects.requireNonNull(targetCompId, "targetCompId");
        Objects.requireNonNull(rawMessage, "rawMessage");
    }

    @Override
    public String toString() {
        // Deliberately omits rawMessage to keep log lines short.
        return "NewOrder[clOrdId=" + clOrdId + ", symbol=" + symbol + ", side=" + side + ", qty=" + orderQty
                + ", ordType=" + ordType + ", price=" + price + ", sender=" + senderCompId + ", seq=" + msgSeqNum + "]";
    }
}
