package io.smallrye.reactive.messaging.pulsar;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@Singleton
class PulsarBrokerConfig {

    @Inject
    @ConfigProperty(name = "pulsar.servers", defaultValue = "pulsar://localhost:6650")
    private String servers;

    String getServers() {
        return servers;
    }
}
