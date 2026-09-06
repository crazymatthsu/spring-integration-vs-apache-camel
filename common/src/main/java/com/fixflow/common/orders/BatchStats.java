package com.fixflow.common.orders;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Counters recorded by the batch-insert step of use case 1, asserted by the integration tests. */
public final class BatchStats {

    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong rows = new AtomicLong();
    private final AtomicInteger largestBatch = new AtomicInteger();

    public void recordBatch(int size) {
        batches.incrementAndGet();
        rows.addAndGet(size);
        largestBatch.accumulateAndGet(size, Math::max);
    }

    public long batches() {
        return batches.get();
    }

    public long rows() {
        return rows.get();
    }

    public int largestBatch() {
        return largestBatch.get();
    }

    @Override
    public String toString() {
        return "BatchStats[batches=" + batches + ", rows=" + rows + ", largestBatch=" + largestBatch + "]";
    }
}
