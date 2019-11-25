package io.smallrye.reactive.messaging.pulsar.producer;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

import java.util.Objects;

/**
 * Creates handlers for sink channels.
 *
 * @see PulsarSink
 */
@Singleton
public class PulsarSinkFactory {

    private final ProducerFactory producerFactory;

    @Inject
    public PulsarSinkFactory(ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    public PulsarSink create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        Objects.requireNonNull(config);
        Objects.requireNonNull(pulsarClient);

        Producer<byte[]> producer = producerFactory.create(config, pulsarClient);
        return new PulsarSink(producer);
    }
}
