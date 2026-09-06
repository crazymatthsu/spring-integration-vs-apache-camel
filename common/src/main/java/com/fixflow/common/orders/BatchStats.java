package com.fixflow.common.orders;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Counters recorded by the batch-insert step of use case 1, asserted by the integration tests. */
public final class BatchStats {

    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong rows = new AtomicLong();
    private final AtomicInteger largestBatch = new AtomicInteger();
    private final AtomicLong fallbacks = new AtomicLong();
    private final AtomicLong failedRows = new AtomicLong();

    /** A batch was inserted and acknowledged; {@code size} is the number of records acknowledged. */
    public void recordBatch(int size) {
        batches.incrementAndGet();
        rows.addAndGet(size);
        largestBatch.accumulateAndGet(size, Math::max);
    }

    /** A batch insert failed and the rows were inserted one by one; {@code failed} of them could not be inserted. */
    public void recordFallback(int failed) {
        fallbacks.incrementAndGet();
        failedRows.addAndGet(failed);
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

    public long fallbacks() {
        return fallbacks.get();
    }

    public long failedRows() {
        return failedRows.get();
    }

    @Override
    public String toString() {
        return "BatchStats[batches=" + batches + ", rows=" + rows + ", largestBatch=" + largestBatch
                + ", fallbacks=" + fallbacks + ", failedRows=" + failedRows + "]";
    }
}
