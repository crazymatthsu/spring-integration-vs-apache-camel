package com.fixflow.support.kafka;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Listener containers whose records must be acknowledged explicitly, from any thread, in any order.
 * <p>
 * {@code AckMode.MANUAL} + {@code asyncAcks=true}: an {@link org.springframework.kafka.support.Acknowledgment}
 * invoked on a worker thread is queued, and the consumer thread commits a partition's offset once every earlier
 * offset of that partition has been acknowledged. While acknowledgments of the last poll are outstanding the
 * consumer is paused, which gives natural back-pressure to the asynchronous processing behind it.
 */
public final class ManualAckContainers {

    private ManualAckContainers() {
    }

    public static <K, V> ConcurrentMessageListenerContainer<K, V> manualAsyncAck(
            ConsumerFactory<K, V> consumerFactory, String topic, String groupId, int concurrency) {

        ContainerProperties properties = new ContainerProperties(topic);
        properties.setGroupId(groupId);
        properties.setAckMode(ContainerProperties.AckMode.MANUAL);
        properties.setAsyncAcks(true);

        ConcurrentMessageListenerContainer<K, V> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, properties);
        container.setConcurrency(concurrency);
        return container;
    }
}
