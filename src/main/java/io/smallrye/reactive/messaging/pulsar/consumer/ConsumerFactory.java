package io.smallrye.reactive.messaging.pulsar.consumer;

import javax.inject.Singleton;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import io.smallrye.reactive.messaging.pulsar.PulsarChannelConfig;

@Singleton
public class ConsumerFactory {

    Consumer create(PulsarChannelConfig config, PulsarClient pulsarClient) throws PulsarClientException {
        String topic = config.getTopicOrFail();
        String subscription = config.getTopicSubscriptionNameOrFail();

        return pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();
    }
}
