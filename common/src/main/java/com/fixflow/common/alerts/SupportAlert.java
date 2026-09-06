package com.fixflow.common.alerts;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Something the support team has to look at: rows that were skipped after the batch insert fell back to one-by-one
 * inserts. One alert is raised per affected batch and lists every skipped row.
 *
 * @param raisedAt when the alert was raised
 * @param source   the application raising it ({@code spring.application.name})
 * @param summary  one line saying what happened
 * @param failures the skipped rows
 */
public record SupportAlert(Instant raisedAt, String source, String summary, List<InsertFailure> failures) {

    public SupportAlert {
        Objects.requireNonNull(raisedAt, "raisedAt");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(summary, "summary");
        failures = List.copyOf(failures);
    }

    public static SupportAlert now(String source, String summary, List<InsertFailure> failures) {
        return new SupportAlert(Instant.now(), source, summary, failures);
    }

    /** Single-line form used for the log and for the Kafka topic, easy to match in log-based alerting. */
    public String toLogLine() {
        return "SUPPORT-ALERT source=" + source + " at=" + raisedAt + " summary=\"" + summary
                + "\" failures=" + failures;
    }
}
