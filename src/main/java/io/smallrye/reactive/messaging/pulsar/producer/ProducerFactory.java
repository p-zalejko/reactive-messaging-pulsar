package io.smallrye.reactive.messaging.pulsar.producer;

import javax.inject.Singleton;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

@Singleton
public class ProducerFactory {

    Producer<?> create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        String topic = config.getTopicOrFail();
        Schema<?> messageSchema = config.getMessageSchema();

        return pulsarClient.newProducer(messageSchema)
                .topic(topic)
                .create();
    }
}
