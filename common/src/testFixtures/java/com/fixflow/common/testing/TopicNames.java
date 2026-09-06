package com.fixflow.common.testing;

import java.util.UUID;

/** Unique topic / consumer-group names so that test runs never see each other's records. */
public final class TopicNames {

    private TopicNames() {
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
