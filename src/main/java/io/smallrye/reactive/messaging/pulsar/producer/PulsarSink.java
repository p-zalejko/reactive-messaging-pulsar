package io.smallrye.reactive.messaging.pulsar.producer;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class PulsarSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSink.class);
  private final SubscriberBuilder<? extends Message<?>, Void> subscriber;
  private final MessageSender sender;

  public PulsarSink(MessageSender sender) {
    this.sender  = Objects.requireNonNull(sender);
    subscriber = ReactiveStreams.<Message<?>>builder()
      .flatMapCompletionStage(sender::send)
      .onError(t -> LOGGER.error("Unable to dispatch message to Kafka", t))
      .ignore();
  }

  public SubscriberBuilder<? extends Message<?>, Void> getSink() {
    return subscriber;
  }

  public void closeQuietly() {
    CountDownLatch latch = new CountDownLatch(1);
    try {
      sender.close();
    } catch (Throwable e) {
      LOGGER.debug("An error has been caught while closing the Kafka Write Stream", e);
      latch.countDown();
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
