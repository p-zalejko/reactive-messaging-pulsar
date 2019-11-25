package io.smallrye.reactive.messaging.pulsar.producer;

import javax.inject.Singleton;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

@Singleton
public class ProducerFactory {

    Producer<byte[]> create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        String topic = config.getTopicOrFail();
        return pulsarClient.newProducer()
                .topic(topic)
                .create();
    }
}
