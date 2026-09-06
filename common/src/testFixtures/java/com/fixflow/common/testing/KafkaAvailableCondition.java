package com.fixflow.common.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Backs {@link RequiresKafka}: a plain TCP connect to the first bootstrap server. */
public final class KafkaAvailableCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String first = KafkaTestSupport.bootstrapServers().split(",")[0].trim();
        int colon = first.lastIndexOf(':');
        String host = colon > 0 ? first.substring(0, colon) : first;
        int port = colon > 0 ? Integer.parseInt(first.substring(colon + 1)) : 9092;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            return ConditionEvaluationResult.enabled("Kafka reachable at " + first);
        }
        catch (IOException e) {
            return ConditionEvaluationResult.disabled(
                    "Kafka is not reachable at " + first + " - start it with ./gradlew kafkaUp (" + e.getMessage() + ")");
        }
    }
}
