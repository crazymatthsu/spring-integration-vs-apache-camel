package com.fixflow.common.orders;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.fixflow.common.fix.FixMessageParser;
import com.fixflow.common.fix.NewOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The business logic of use case 2, shared verbatim by the Spring Integration and Camel handlers:
 * parse the FIX order coming from a source topic (algo or dma) and record what was processed.
 * <p>
 * Thread-safe: both demos call it from several worker threads.
 */
public final class OrderProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);

    private final FixMessageParser parser;
    private final Map<String, LongAdder> processedBySource = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> failedBySource = new ConcurrentHashMap<>();
    private final Set<String> threadNames = ConcurrentHashMap.newKeySet();

    public OrderProcessor(FixMessageParser parser) {
        this.parser = parser;
    }

    /**
     * @param source the Kafka topic the message came from
     * @param rawFix the FIX message
     * @return the parsed order
     * @throws com.fixflow.common.fix.FixParseException if the message is not a valid order
     */
    public NewOrder process(String source, String rawFix) {
        threadNames.add(Thread.currentThread().getName());
        try {
            NewOrder order = parser.parseNewOrderSingle(rawFix);
            processedBySource.computeIfAbsent(source, s -> new LongAdder()).increment();
            log.info("[{}] processed {} on {}", source, order, Thread.currentThread().getName());
            return order;
        }
        catch (RuntimeException e) {
            failedBySource.computeIfAbsent(source, s -> new LongAdder()).increment();
            throw e;
        }
    }

    public long processed(String source) {
        LongAdder adder = processedBySource.get(source);
        return adder == null ? 0 : adder.sum();
    }

    public long failed(String source) {
        LongAdder adder = failedBySource.get(source);
        return adder == null ? 0 : adder.sum();
    }

    /** Names of every thread that invoked {@link #process}; lets tests prove the hand-off to worker threads. */
    public Set<String> threadNames() {
        return Set.copyOf(threadNames);
    }
}
