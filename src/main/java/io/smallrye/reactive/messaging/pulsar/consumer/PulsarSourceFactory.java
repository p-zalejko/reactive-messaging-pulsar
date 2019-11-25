package io.smallrye.reactive.messaging.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PulsarSourceFactory {

  private final ConsumerFactory consumerFactory;

  @Inject
  public PulsarSourceFactory(ConsumerFactory consumerFactory) {
    this.consumerFactory = consumerFactory;
  }

  public PulsarSource create(Config config, PulsarClient pulsarClient) throws PulsarClientException {
    Consumer consumer = consumerFactory.create(config, pulsarClient);
    MessageConsumer messageConsumer = new MessageConsumer(consumer);
    return new PulsarSource(messageConsumer);
  }

}
