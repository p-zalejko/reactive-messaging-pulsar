package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;

/**
 * Handles source channels, annotated with the {@link org.eclipse.microprofile.reactive.messaging.Incoming} annotation.
 */
public class PulsarSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);
    private static final long DEFAULT_TIMEOUT = 15;

    private final PublisherBuilder<? extends Message<?>> source;
    private final Consumer consumer;
    private final ExecutorService executor;

    PulsarSource(Consumer consumer, ExecutorService executor) {
        this.consumer = Objects.requireNonNull(consumer);
        this.executor = Objects.requireNonNull(executor);

        Flowable<? extends Message<?>> flowable = Flowable.create(emitter -> receiveNext(consumer, emitter),
                BackpressureStrategy.BUFFER);
        this.source = ReactiveStreams.fromPublisher(flowable);
    }

    public PublisherBuilder<? extends Message<?>> getSource() {
        return source;
    }

    public void closeQuietly() {
        try {
            source.cancel();
            consumer.unsubscribe();
            executor.shutdown();
            executor.awaitTermination(DEFAULT_TIMEOUT, TimeUnit.SECONDS);

            consumer.close();
        } catch (Exception e) {
            LOGGER.error("An exception has been caught while closing the consumer", e);
        }
    }

    private void receiveNext(Consumer<Object> subscribe, FlowableEmitter subject) {
        CompletableFuture<org.apache.pulsar.client.api.Message<Object>> messageCompletableFuture = subscribe.receiveAsync();
        messageCompletableFuture.handle((msg, exception) -> {
            //TODO: handle exception
            handleNewMessage(subscribe, subject, msg);
            return null;
        });
    }

    private void handleNewMessage(Consumer<Object> subscribe, FlowableEmitter subject,
            org.apache.pulsar.client.api.Message<Object> msg) {
        if (subscribe.isConnected()) {
            try {
                subject.onNext(org.eclipse.microprofile.reactive.messaging.Message.of(msg.getValue()));
                subscribe.acknowledge(msg);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }

            if (!executor.isShutdown() && !executor.isTerminated()) {
                // scheduler for the next message
                executor.execute(() -> receiveNext(subscribe, subject));
            } else {
                LOGGER.warn("Handling of next messages will not be started. Consumer is being closed.");
            }
        } else {
            LOGGER.warn("Did not handle a new message. Consumer is already disconnected.");
        }
    }
}
