package com.fixflow.common.fix;

import java.math.BigDecimal;
import java.time.ZoneOffset;

import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.InvalidMessage;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageUtils;
import quickfix.ValidationSettings;
import quickfix.field.ClOrdID;
import quickfix.field.MsgSeqNum;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TransactTime;
import quickfix.fix42.NewOrderSingle;

/**
 * Parses and validates raw FIX 4.2 strings with QuickFIX/J.
 * <p>
 * Validation covers body length, checksum, required fields and enumerated tag values as defined by the
 * FIX 4.2 data dictionary. Instances are thread-safe and cheap to share.
 */
public final class FixMessageParser {

    private static final String DICTIONARY = "FIX42.xml";

    private final DataDictionary dictionary;
    private final ValidationSettings validationSettings = new ValidationSettings();
    private final MessageFactory messageFactory = new quickfix.fix42.MessageFactory();

    public FixMessageParser() {
        try {
            this.dictionary = new DataDictionary(DICTIONARY);
        }
        catch (ConfigError e) {
            throw new IllegalStateException("Cannot load FIX data dictionary " + DICTIONARY, e);
        }
    }

    /**
     * @param raw the FIX message exactly as received (SOH-delimited)
     * @return the parsed order
     * @throws FixParseException if the message is not a valid FIX 4.2 NewOrderSingle
     */
    public NewOrder parseNewOrderSingle(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new FixParseException("Empty FIX message");
        }
        try {
            String msgType = MessageUtils.getMessageType(raw);
            if (!NewOrderSingle.MSGTYPE.equals(msgType)) {
                throw new FixParseException("Expected NewOrderSingle (35=D) but got 35=" + msgType);
            }
            Message message = MessageUtils.parse(messageFactory, dictionary, validationSettings, raw);
            dictionary.validate(message, validationSettings);
            if (!(message instanceof NewOrderSingle order)) {
                throw new FixParseException("Message factory produced " + message.getClass().getName());
            }
            return toNewOrder(order, raw);
        }
        catch (InvalidMessage | FieldNotFound | IncorrectTagValue | IncorrectDataFormat e) {
            throw new FixParseException("Invalid FIX 4.2 NewOrderSingle: " + e.getMessage(), e);
        }
    }

    private static NewOrder toNewOrder(NewOrderSingle order, String raw) throws FieldNotFound {
        Message.Header header = order.getHeader();
        BigDecimal price = order.isSetField(Price.FIELD) ? order.getDecimal(Price.FIELD) : null;
        return new NewOrder(
                order.getString(ClOrdID.FIELD),
                order.getString(Symbol.FIELD),
                order.getChar(Side.FIELD),
                order.getDecimal(OrderQty.FIELD),
                order.getChar(OrdType.FIELD),
                price,
                order.getUtcTimeStamp(TransactTime.FIELD).toInstant(ZoneOffset.UTC),
                header.getString(SenderCompID.FIELD),
                header.getString(TargetCompID.FIELD),
                header.getInt(MsgSeqNum.FIELD),
                raw);
    }
}
