package io.smallrye.reactive.messaging.pulsar;

import java.util.Objects;
import java.util.Optional;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.eclipse.microprofile.config.Config;

/**
 * Provides configuration parameters for channels (input and output).
 */
public class PulsarChannelConfig {

    private final Config config;

    PulsarChannelConfig(Config config) {
        this.config = Objects.requireNonNull(config);
    }

    public String getTopicOrFail() {
        return getProperty("topic");
    }

    public String getTopicSubscriptionNameOrFail() {
        return getProperty("topic-subscription");
    }

    public Schema<?> getMessageSchema() {
        Optional<String> schemaClass = getOptionalProperty("schema");
        if (schemaClass.isPresent()) {
            try {
                Class<?> aClass = Class.forName(schemaClass.get());
                return (Schema) aClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        return BytesSchema.of();
    }

    private String getProperty(String key) {
        return getOptionalProperty(key)
                .orElseThrow(() -> new IllegalArgumentException(String.format("%s attribute must be set", key)));
    }

    private Optional<String> getOptionalProperty(String key) {
        return config.getOptionalValue(key, String.class);
    }

}
