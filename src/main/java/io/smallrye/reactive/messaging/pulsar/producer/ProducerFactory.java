package io.smallrye.reactive.messaging.pulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.Config;

import javax.inject.Singleton;

@Singleton
public class ProducerFactory {

  Producer<byte[]> create(Config config, PulsarClient pulsarClient) throws PulsarClientException {
    String topic = getTopicOrFail(config);
    return pulsarClient.newProducer()
      .topic(topic)
      .create();
  }

  private String getTopicOrFail(Config config) {
    return getProperty(config, "topic");
  }


  private String getProperty(Config config, String key) {
    return config.getOptionalValue(key, String.class)
      .orElseThrow(() -> new IllegalArgumentException(String.format("%s attribute must be set", key)));
  }
}
