package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

@Singleton
public class PulsarSourceFactory {

    private final ConsumerFactory consumerFactory;

    @Inject
    public PulsarSourceFactory(ConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public PulsarSource create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        Consumer consumer = consumerFactory.create(config, pulsarClient);
        return new PulsarSource(consumer, Executors.newSingleThreadExecutor());
    }

}
