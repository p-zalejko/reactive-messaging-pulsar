package io.smallrye.reactive.messaging.pulsar;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private List<Integer> list = new ArrayList<>();
    private List<Message<Object>> kafka = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
//    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message process(Message<Object> input) {
        kafka.add(input);
        return Message.of("input.getPayload() + 1");
    }

    @Incoming("sinka")
    public void sink(Object val) {
        list.add(1);
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<Message<Object>> getKafkaMessages() {
        return kafka;
    }
}
