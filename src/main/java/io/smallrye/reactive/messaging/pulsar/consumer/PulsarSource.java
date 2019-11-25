package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;

public class PulsarSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);

    private final PublisherBuilder<? extends Message<?>> source;
    private final Consumer consumer;
    private final ExecutorService executor;

    PulsarSource(Consumer consumer, ExecutorService executor) {
        this.consumer = Objects.requireNonNull(consumer);
        this.executor = Objects.requireNonNull(executor);

        Flowable flowable = Flowable.create(emitter -> receiveNext(consumer, emitter), BackpressureStrategy.BUFFER);
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
            executor.awaitTermination(10L, TimeUnit.SECONDS);

            consumer.close();
        } catch (Throwable e) {
            LOGGER.debug("An exception has been caught while closing the Kafka consumer", e);
        }
    }

    private void receiveNext(Consumer<Object> subscribe, FlowableEmitter subject) {
        CompletableFuture<org.apache.pulsar.client.api.Message<Object>> messageCompletableFuture = subscribe.receiveAsync();
        messageCompletableFuture.handle((msg, exception) -> {
            handleNewMessage(subscribe, subject, msg);
            return null;
        });
    }

    private void handleNewMessage(Consumer<Object> subscribe, FlowableEmitter subject,
            org.apache.pulsar.client.api.Message<Object> msg) {
        if (subscribe.isConnected()) {
            try {
                subject.onNext(org.eclipse.microprofile.reactive.messaging.Message.of(msg));
                subscribe.acknowledge(msg);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }

            if (!executor.isShutdown() && !executor.isTerminated()) {
                // scheduler for the next message
                executor.execute(() -> receiveNext(subscribe, subject));
            }
        } else {
            LOGGER.warn("Did not handle a new message. Consumer is already disconnected.");
        }
    }
}
