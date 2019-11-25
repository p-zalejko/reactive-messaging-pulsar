package io.smallrye.reactive.messaging.pulsar;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private List<Object> sinkTopicMessages = new ArrayList<>();
    private List<Message<Object>> dataTopicMessages = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sinkOut")
    //    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message process(Message<Object> input) {
        dataTopicMessages.add(input);
        return Message.of("input.getPayload() + 1");
    }

    @Incoming("sinkIn")
    public void sink(Object val) {
        sinkTopicMessages.add(val);
    }

    List<Message<Object>> getDataTopicMessages() {
        return dataTopicMessages;
    }

    List<Object> getSinkTopicMessages() {
        return sinkTopicMessages;
    }
}
