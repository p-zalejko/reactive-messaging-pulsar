package io.smallrye.reactive.messaging.pulsar;

import java.util.Objects;

import org.eclipse.microprofile.config.Config;

public class PulsarChannelConfig {

    private final Config config;

    PulsarChannelConfig(Config config) {
        this.config = Objects.requireNonNull(config);
    }

    public String getTopicOrFail() {
        return getProperty(config, "topic");
    }

    public String getTopicSubscriptionNameOrFail() {
        return getProperty(config, "topic-subscription");
    }

    private String getProperty(Config config, String key) {
        return config.getOptionalValue(key, String.class)
                .orElseThrow(() -> new IllegalArgumentException(String.format("%s attribute must be set", key)));
    }

}
