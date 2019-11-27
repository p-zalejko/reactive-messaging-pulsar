package io.smallrye.reactive.messaging.pulsar.helper;

import java.util.HashMap;
import java.util.Map;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;

public class ConfigHelper {

    public static MapBasedConfig getDefaultConfig(String pulsarUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put("pulsar.servers", pulsarUrl);

        String sourcePrefix = "mp.messaging.incoming.data.";
        config.put(sourcePrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sourcePrefix + "topic", "data");
        config.put(sourcePrefix + "topic-subscription", "foo");

        String sinkOutPrefix = "mp.messaging.outgoing.sinkOut.";
        config.put(sinkOutPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkOutPrefix + "topic", "sinkTopic");

        String sinkInPrefix = "mp.messaging.incoming.sinkIn.";
        config.put(sinkInPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkInPrefix + "topic", "sinkTopic");
        config.put(sinkInPrefix + "topic-subscription", "foo");

        return new MapBasedConfig(config);
    }

    /**
     * Creates a configuration with message schemas
     */
    public static MapBasedConfig getConfig(String pulsarUrl, Class<?> dataSchema, Class<?> sinkSchema) {
        Map<String, Object> config = new HashMap<>();
        config.put("pulsar.servers", pulsarUrl);

        String sourcePrefix = "mp.messaging.incoming.data.";
        config.put(sourcePrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sourcePrefix + "topic", "data");
        config.put(sourcePrefix + "topic-subscription", "foo");
        config.put(sourcePrefix + "schema", dataSchema.getName());

        String sinkOutPrefix = "mp.messaging.outgoing.sinkOut.";
        config.put(sinkOutPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkOutPrefix + "topic", "sinkTopic");
        config.put(sinkOutPrefix + "schema", sinkSchema.getName());

        String sinkInPrefix = "mp.messaging.incoming.sinkIn.";
        config.put(sinkInPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkInPrefix + "topic", "sinkTopic");
        config.put(sinkInPrefix + "topic-subscription", "foo");
        config.put(sinkInPrefix + "schema", sinkSchema.getName());

        return new MapBasedConfig(config);
    }
}
