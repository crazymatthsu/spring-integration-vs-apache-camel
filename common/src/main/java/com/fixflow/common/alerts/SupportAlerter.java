package com.fixflow.common.alerts;

/**
 * Notifies the support team. The demos ship {@link LoggingSupportAlerter} (an ERROR log line with a marker that
 * log-based alerting can match) and {@link KafkaSupportAlerter} (a message on a support topic that on-call tooling
 * can consume), combined by {@link CompositeSupportAlerter}. Plug in e-mail, PagerDuty, Slack or anything else by
 * implementing this interface and adding it to the composite.
 */
public interface SupportAlerter {

    void raise(SupportAlert alert);
}
