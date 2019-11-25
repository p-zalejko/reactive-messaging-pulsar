package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.enterprise.inject.Default;
import javax.inject.Singleton;

@Singleton
@Default
class PulsarClientFactory {

  PulsarClient createClient(String servers) throws PulsarClientException {
    return PulsarClient.builder()
      .serviceUrl(servers)
      .build();
  }
}
