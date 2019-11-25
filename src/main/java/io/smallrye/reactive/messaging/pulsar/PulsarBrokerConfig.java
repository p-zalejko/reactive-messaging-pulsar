package io.smallrye.reactive.messaging.pulsar;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Provides broker configuration, e.g. a URL to the pulsar broker.
 */
@Singleton
class PulsarBrokerConfig {

    @Inject
    @ConfigProperty(name = "pulsar.servers", defaultValue = "pulsar://localhost:6650")
    private String servers;

    String getServers() {
        return servers;
    }
}
