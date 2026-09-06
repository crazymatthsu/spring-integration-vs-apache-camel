package com.fixflow.common.fix;

/** Thrown when a Kafka record does not contain a valid FIX 4.2 NewOrderSingle. */
public class FixParseException extends RuntimeException {

    public FixParseException(String message) {
        super(message);
    }

    public FixParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
