package io.smallrye.reactive.messaging.pulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PulsarSinkFactory {

  private final ProducerFactory producerFactory;

  @Inject
  public PulsarSinkFactory(ProducerFactory producerFactory) {
    this.producerFactory = producerFactory;
  }

  public PulsarSink create(Config config, PulsarClient pulsarClient) throws PulsarClientException {
    Producer<byte[]> producer = producerFactory.create(config, pulsarClient);
    MessageSender sender = new MessageSender(producer);
    return new PulsarSink(sender);
  }
}
