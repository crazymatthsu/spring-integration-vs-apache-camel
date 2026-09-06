package com.fixflow.common.alerts;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Writes every alert as one ERROR line carrying the {@code SUPPORT_ALERT} marker, and keeps the most recent alerts
 * in memory so that tests (or an operator with a debugger) can inspect them.
 */
public final class LoggingSupportAlerter implements SupportAlerter {

    public static final Marker MARKER = MarkerFactory.getMarker("SUPPORT_ALERT");

    private static final Logger log = LoggerFactory.getLogger(LoggingSupportAlerter.class);
    private static final int HISTORY = 100;

    private final Deque<SupportAlert> recent = new ArrayDeque<>();

    @Override
    public void raise(SupportAlert alert) {
        log.error(MARKER, "{}", alert.toLogLine());
        synchronized (recent) {
            recent.addLast(alert);
            while (recent.size() > HISTORY) {
                recent.removeFirst();
            }
        }
    }

    /** The last alerts raised, oldest first. */
    public List<SupportAlert> recent() {
        synchronized (recent) {
            return List.copyOf(recent);
        }
    }
}
