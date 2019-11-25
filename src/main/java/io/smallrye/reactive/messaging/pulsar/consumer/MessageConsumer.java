package io.smallrye.reactive.messaging.pulsar.consumer;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class MessageConsumer {

    private final Consumer consumer;
    private final Flowable<Message<?>> flowable;
    private final ExecutorService executor;

    MessageConsumer(Consumer consumer) {
        this(consumer, Executors.newSingleThreadExecutor());
    }

    MessageConsumer(Consumer consumer, ExecutorService executor) {
        this.consumer = Objects.requireNonNull(consumer);
        this.flowable = Flowable.create(emitter -> receiveNext(consumer, emitter), BackpressureStrategy.BUFFER);
        this.executor = Objects.requireNonNull(executor);
    }

    Flowable<Message<?>> getAsFlowable() {
        return flowable;
    }

    void close() throws PulsarClientException {
        consumer.unsubscribe();
        try {
            executor.shutdown();
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        consumer.close();
    }

    private void receiveNext(Consumer<Object> subscribe, FlowableEmitter subject) {
        CompletableFuture<org.apache.pulsar.client.api.Message<Object>> messageCompletableFuture = subscribe.receiveAsync();
        messageCompletableFuture.handle((msg, exception) -> {
            handleNewMessage(subscribe, subject, msg);
            return null;
        });
    }

    private void handleNewMessage(Consumer<Object> subscribe, FlowableEmitter subject, org.apache.pulsar.client.api.Message<Object> msg) {
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
        }


    }
}
