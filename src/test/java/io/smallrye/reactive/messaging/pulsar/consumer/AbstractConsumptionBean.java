package io.smallrye.reactive.messaging.pulsar.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public abstract class AbstractConsumptionBean<T, K> {

    private final List<T> dataTopicMessages;
    private final List<K> sinkTopicMessages;
    private final Function<T, K> dataToSinkConverter;

    AbstractConsumptionBean(Function<T, K> dataToSinkConverter) {
        this.dataToSinkConverter = dataToSinkConverter;
        this.dataTopicMessages = new ArrayList<>();
        this.sinkTopicMessages = new ArrayList<>();
    }

    @Incoming("data")
    @Outgoing("sinkOut")
    public Message process(Message<T> input) {
        T payload = input.getPayload();
        dataTopicMessages.add(payload);
        return Message.of(dataToSinkConverter.apply(payload));
    }

    @Incoming("sinkIn")
    public void sink(K val) {
        sinkTopicMessages.add(val);
    }

    public List<T> getDataTopicMessages() {
        return dataTopicMessages;
    }

    public List<K> getSinkTopicMessages() {
        return sinkTopicMessages;
    }
}
