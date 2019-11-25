package io.smallrye.reactive.messaging.pulsar.producer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class MessageSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);
  private final Producer producer;

  public MessageSender(Producer<?> producer) {
    this.producer = Objects.requireNonNull(producer);
  }

  CompletableFuture<Message> send(Message message){
    try {
      MessageId send = producer.send("foo".getBytes());
      return CompletableFuture.completedFuture(message);
    } catch (RuntimeException | PulsarClientException e) {
      LOGGER.error("Unable to send a record to Kafka ", e);
      return CompletableFuture.completedFuture(message);
    }
  }

  public void close() throws PulsarClientException {
    producer.close();
  }
}
