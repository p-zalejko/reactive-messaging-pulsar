package io.smallrye.reactive.messaging.pulsar.producer;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

@Singleton
public class PulsarSinkFactory {

    private final ProducerFactory producerFactory;

    @Inject
    public PulsarSinkFactory(ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    public PulsarSink create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        Producer<byte[]> producer = producerFactory.create(config, pulsarClient);
        return new PulsarSink(producer);
    }
}
