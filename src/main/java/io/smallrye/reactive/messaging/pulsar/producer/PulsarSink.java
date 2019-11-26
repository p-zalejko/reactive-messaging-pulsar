package io.smallrye.reactive.messaging.pulsar.producer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Producer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles sink channels, annotated with the {@link org.eclipse.microprofile.reactive.messaging.Outgoing} annotation.
 */
public class PulsarSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSink.class);

    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;
    private final Producer producer;

    PulsarSink(Producer<?> producer) {
        this.producer = Objects.requireNonNull(producer);
        this.subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(this::send)
                .onError(t -> LOGGER.error("Unable to dispatch message", t))
                .ignore();
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSink() {
        return subscriber;
    }

    public void closeQuietly() {
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            LOGGER.debug("An error has been caught while closing the Write Stream", e);
        }
    }

    private CompletableFuture<Message> send(Message message) {
        if (producer.isConnected()) {
            try {
                producer.send(message.getPayload());
                return CompletableFuture.completedFuture(message);
            } catch (Exception e) {
                LOGGER.error("Unable to send a record", e);
                return CompletableFuture.completedFuture(null);
            }
        } else {
            LOGGER.warn("Message has not been sent. Producer is already disconnected");
            return CompletableFuture.completedFuture(null);
        }
    }
}
