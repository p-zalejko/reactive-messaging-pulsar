package io.smallrye.reactive.messaging.pulsar;

import java.util.Objects;

import javax.enterprise.inject.Default;
import javax.inject.Singleton;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Creates a {@link PulsarClient} relying on the configuration parameters provided by {@link PulsarBrokerConfig}.
 */
@Singleton
@Default
class PulsarClientFactory {

    PulsarClient createClient(PulsarBrokerConfig config) throws PulsarClientException {
        Objects.requireNonNull(config);

        return PulsarClient.builder()
                .serviceUrl(config.getServers())
                .build();
    }
}
