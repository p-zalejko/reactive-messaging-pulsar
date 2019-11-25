package io.smallrye.reactive.messaging.pulsar.producer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSink.class);

    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;
    private final Producer producer;

    public PulsarSink(Producer producer) {
        this.producer = Objects.requireNonNull(producer);
        this.subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(this::send)
                .onError(t -> LOGGER.error("Unable to dispatch message to Kafka", t))
                .ignore();
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSink() {
        return subscriber;
    }

    public void closeQuietly() {
        try {
            producer.flush();
            producer.close();
        } catch (Throwable e) {
            LOGGER.debug("An error has been caught while closing the Kafka Write Stream", e);
        }
    }

    private CompletableFuture<Message> send(Message message) {
        if (producer.isConnected()) {
            try {
                MessageId send = producer.send("foo".getBytes());
                return CompletableFuture.completedFuture(message);
            } catch (RuntimeException | PulsarClientException e) {
                LOGGER.error("Unable to send a record to Kafka ", e);
                return CompletableFuture.completedFuture(message);
            }
        } else {
            LOGGER.warn("Message has not been sent. Producer is already disconnected");
            return CompletableFuture.completedFuture(null);
        }
    }
}
