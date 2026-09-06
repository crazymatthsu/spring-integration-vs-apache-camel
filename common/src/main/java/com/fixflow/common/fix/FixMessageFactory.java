package com.fixflow.common.fix;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.HandlInst;
import quickfix.field.MsgSeqNum;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.SenderCompID;
import quickfix.field.SendingTime;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix42.Heartbeat;
import quickfix.fix42.NewOrderSingle;

/**
 * Builds well-formed FIX 4.2 messages (BodyLength and CheckSum are computed by QuickFIX/J).
 * Used by the demo producer and by the tests; the receiving side never depends on it.
 */
public final class FixMessageFactory {

    private final String senderCompId;
    private final String targetCompId;
    private final AtomicInteger msgSeqNum = new AtomicInteger();

    public FixMessageFactory(String senderCompId, String targetCompId) {
        this.senderCompId = senderCompId;
        this.targetCompId = targetCompId;
    }

    /** A limit order when {@code price} is given, otherwise a market order. */
    public String newOrderSingle(String clOrdId, String symbol, char side, BigDecimal orderQty, BigDecimal price) {
        NewOrderSingle order = new NewOrderSingle(
                new ClOrdID(clOrdId),
                new HandlInst(HandlInst.AUTOMATED_EXECUTION_NO_INTERVENTION),
                new Symbol(symbol),
                new Side(side),
                new TransactTime(utcNow()),
                new OrdType(price == null ? OrdType.MARKET : OrdType.LIMIT));
        order.setDecimal(OrderQty.FIELD, orderQty);
        if (price != null) {
            order.setDecimal(Price.FIELD, price);
        }
        order.set(new TimeInForce(TimeInForce.DAY));
        fillHeader(order);
        return order.toString();
    }

    /** A message that is valid FIX but not an order (35=0). */
    public String heartbeat() {
        Heartbeat heartbeat = new Heartbeat();
        fillHeader(heartbeat);
        return heartbeat.toString();
    }

    /** Breaks the checksum of an otherwise valid message. */
    public static String corruptChecksum(String fixMessage) {
        int idx = fixMessage.lastIndexOf("10=");
        if (idx < 0) {
            throw new IllegalArgumentException("No CheckSum field in " + fixMessage);
        }
        return fixMessage.substring(0, idx) + "10=000" + fixMessage.substring(fixMessage.length() - 1);
    }

    private void fillHeader(Message message) {
        Message.Header header = message.getHeader();
        header.setString(SenderCompID.FIELD, senderCompId);
        header.setString(TargetCompID.FIELD, targetCompId);
        header.setInt(MsgSeqNum.FIELD, msgSeqNum.incrementAndGet());
        header.setUtcTimeStamp(SendingTime.FIELD, utcNow(), true);
    }

    private static LocalDateTime utcNow() {
        return LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS);
    }
}
