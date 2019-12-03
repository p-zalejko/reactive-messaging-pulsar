package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;

@ApplicationScoped
public class ConsumptionBytesToStringWithEmitterBean {

    private final List<String> sinkTopicMessages;

    @Inject
    @Channel("sinkOut")
    Emitter<String> emitter;

    ConsumptionBytesToStringWithEmitterBean() {
        this.sinkTopicMessages = new ArrayList<>();
    }

    public void sendMessage(int i) {
        emitter.send("foo " + i);
    }

    @Incoming("sinkIn")
    public void sink(String val) {
        sinkTopicMessages.add(val);
    }

    public List<String> getSinkTopicMessages() {
        return sinkTopicMessages;
    }
}
