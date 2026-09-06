package com.fixflow.common.tools;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.fixflow.common.fix.FixMessageFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Demo tool: publishes FIX 4.2 NewOrderSingle messages to a topic.
 * <pre>
 *   ./gradlew :common:sendOrders -Ptopic=orders -Pcount=100 -PinvalidEvery=25
 * </pre>
 * Every {@code invalidEvery}-th message (0 = never) is sent with a broken checksum to exercise the poison-message path.
 */
public final class FixOrderProducer {

    private static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA");

    private FixOrderProducer() {
    }

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "orders";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int invalidEvery = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        String bootstrap = System.getProperty("fixflow.kafka.bootstrap",
                System.getenv().getOrDefault("FIXFLOW_KAFKA_BOOTSTRAP", "localhost:9092"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        FixMessageFactory factory = new FixMessageFactory("CLIENT1", "BROKER");
        String runId = UUID.randomUUID().toString().substring(0, 8);
        int invalid = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String clOrdId = runId + "-" + i;
                String fix = randomOrder(factory, clOrdId);
                if (invalidEvery > 0 && i % invalidEvery == 0) {
                    fix = FixMessageFactory.corruptChecksum(fix);
                    invalid++;
                }
                producer.send(new ProducerRecord<>(topic, clOrdId, fix));
            }
            producer.flush();
        }
        System.out.printf("Sent %d messages (%d invalid) to topic '%s' on %s%n", count, invalid, topic, bootstrap);
    }

    private static String randomOrder(FixMessageFactory factory, String clOrdId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
        char side = random.nextBoolean() ? '1' : '2';
        BigDecimal qty = BigDecimal.valueOf(100L * (1 + random.nextInt(50)));
        BigDecimal price = random.nextInt(4) == 0 ? null : BigDecimal.valueOf(random.nextInt(10_000, 50_000), 2);
        return factory.newOrderSingle(clOrdId, symbol, side, qty, price);
    }
}
