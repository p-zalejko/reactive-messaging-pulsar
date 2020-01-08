package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.smallrye.reactive.messaging.annotations.Emitter;
import io.reactivex.FlowableSubscriber;
import io.smallrye.reactive.messaging.annotations.Channel;

@ApplicationScoped
public class ConsumptionBytesToStringWithPublisherBean {

    private final SampleSubscriber sampleSubscriber;

    @Inject
    @Channel("sinkIn")
    Publisher<String> publisher;

    @Inject
    @Channel("sinkOut")
    Emitter<String> emitter;
    
    ConsumptionBytesToStringWithPublisherBean() {
        this.sampleSubscriber = new SampleSubscriber();
    }

    @PostConstruct
    public void init() {
        publisher.subscribe(sampleSubscriber);
    }

    public List<String> getSinkTopicMessages() {
        return sampleSubscriber.sinkTopicMessages;
    }

    public void sendMessage(int i) {
        emitter.send("foo " + i);
    }

    private static class SampleSubscriber implements FlowableSubscriber<String> {

        final List<String> sinkTopicMessages;

        SampleSubscriber() {
            this.sinkTopicMessages = new ArrayList<>();
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(String t) {
            this.sinkTopicMessages.add(t);
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
        }

        @Override
        public void onComplete() {
        }
    }
}
