package com.fixflow.common.fix;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixMessageParserTest {

    private final FixMessageParser parser = new FixMessageParser();
    private final FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");

    @Test
    void parsesLimitOrder() {
        String raw = factory.newOrderSingle("ORD-1", "AAPL", '1', new BigDecimal("100"), new BigDecimal("189.50"));

        NewOrder order = parser.parseNewOrderSingle(raw);

        assertThat(order.clOrdId()).isEqualTo("ORD-1");
        assertThat(order.symbol()).isEqualTo("AAPL");
        assertThat(order.side()).isEqualTo('1');
        assertThat(order.orderQty()).isEqualByComparingTo("100");
        assertThat(order.ordType()).isEqualTo('2');
        assertThat(order.price()).isEqualByComparingTo("189.50");
        assertThat(order.senderCompId()).isEqualTo("CLIENT1");
        assertThat(order.targetCompId()).isEqualTo("BROKER");
        assertThat(order.msgSeqNum()).isEqualTo(1);
        assertThat(order.transactTime()).isNotNull();
        assertThat(order.rawMessage()).isEqualTo(raw);
    }

    @Test
    void parsesMarketOrderWithoutPrice() {
        String raw = factory.newOrderSingle("ORD-2", "MSFT", '2', new BigDecimal("50"), null);

        NewOrder order = parser.parseNewOrderSingle(raw);

        assertThat(order.ordType()).isEqualTo('1');
        assertThat(order.price()).isNull();
    }

    @Test
    void rejectsNonOrderMessages() {
        assertThatThrownBy(() -> parser.parseNewOrderSingle(factory.heartbeat()))
                .isInstanceOf(FixParseException.class)
                .hasMessageContaining("35=0");
    }

    @Test
    void rejectsBadChecksum() {
        String raw = FixMessageFactory.corruptChecksum(
                factory.newOrderSingle("ORD-3", "NVDA", '1', new BigDecimal("10"), null));

        assertThatThrownBy(() -> parser.parseNewOrderSingle(raw))
                .isInstanceOf(FixParseException.class)
                .hasMessageContaining("Invalid FIX 4.2 NewOrderSingle");
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> parser.parseNewOrderSingle("this is not FIX"))
                .isInstanceOf(FixParseException.class);
        assertThatThrownBy(() -> parser.parseNewOrderSingle(""))
                .isInstanceOf(FixParseException.class);
    }
}
