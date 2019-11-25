package io.smallrye.reactive.messaging.pulsar;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class PulsarConfig {

  @Inject
  @ConfigProperty(name = "pulsar.servers", defaultValue = "pulsar://localhost:6650")
  private String servers;

  String getServers() {
    return servers;
  }
}
