package io.smallrye.reactive.messaging.pulsar.consumer;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class PulsarSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);

  private final PublisherBuilder<? extends Message<?>> source;
  private final MessageConsumer messageConsumer;

  PulsarSource(MessageConsumer messageConsumer) {
    this.messageConsumer = Objects.requireNonNull(messageConsumer);

    Flowable<Message<?>> integerFlowable = messageConsumer.getAsFlowable();
    this.source = ReactiveStreams.fromPublisher(integerFlowable);
  }

  public PublisherBuilder<? extends Message<?>> getSource() {
    return source;
  }

  public void closeQuietly() {
    try {
      source.cancel();
      messageConsumer.close();
    } catch (Throwable e) {
      LOGGER.debug("An exception has been caught while closing the Kafka consumer", e);
    }
  }


}
