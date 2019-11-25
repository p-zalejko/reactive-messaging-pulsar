package io.smallrye.reactive.messaging.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.Config;

import javax.inject.Singleton;

@Singleton
public class ConsumerFactory {

  Consumer create(Config config, PulsarClient pulsarClient) throws PulsarClientException {
    String topic = getTopicOrFail(config);
    String subscription = getTopicSubscriptionNameOrFail(config);

    return pulsarClient.newConsumer()
      .topic(topic)
      .subscriptionName(subscription)
      .subscribe();
  }

  private String getTopicOrFail(Config config) {
    return getProperty(config, "topic");
  }

  private String getTopicSubscriptionNameOrFail(Config config) {
    return getProperty(config, "topic-subscription");
  }

  private String getProperty(Config config, String key) {
    return config.getOptionalValue(key, String.class)
      .orElseThrow(() -> new IllegalArgumentException(String.format("%s attribute must be set", key)));
  }
}
