package com.fixflow.common.alerts;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fans an alert out to several alerters; one failing alerter does not stop the others. */
public final class CompositeSupportAlerter implements SupportAlerter {

    private static final Logger log = LoggerFactory.getLogger(CompositeSupportAlerter.class);

    private final List<SupportAlerter> alerters;

    public CompositeSupportAlerter(List<SupportAlerter> alerters) {
        this.alerters = List.copyOf(alerters);
    }

    @Override
    public void raise(SupportAlert alert) {
        for (SupportAlerter alerter : alerters) {
            try {
                alerter.raise(alert);
            }
            catch (RuntimeException e) {
                log.error("{} failed to raise {}: {}", alerter.getClass().getSimpleName(), alert.summary(), e.toString());
            }
        }
    }
}
