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
    private List<Message<?>> dataTopicMessages = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sinkOut")
    //    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message process(Message<byte[]> input) {
        byte[] payload = input.getPayload();
        dataTopicMessages.add(input);
        return Message.of(payload);
    }

    @Incoming("sinkIn")
    public void sink(byte[] val) {
        sinkTopicMessages.add(val);
    }

    List<Message<?>> getDataTopicMessages() {
        return dataTopicMessages;
    }

    List<Object> getSinkTopicMessages() {
        return sinkTopicMessages;
    }
}
