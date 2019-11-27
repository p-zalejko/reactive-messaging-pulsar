package io.smallrye.reactive.messaging.pulsar.helper;

import static org.assertj.core.api.Assertions.fail;

import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class TestDataPublisher {

    public void produceTestMessages(PulsarClient pulsarClient, String topic, int count) throws PulsarClientException {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        IntStream.range(0, count)
                .mapToObj(i -> new byte[] { (byte) i })
                .forEach(bytes -> send(producer, bytes));
    }

    private void send(Producer<byte[]> producer, byte[] bytes) {
        try {
            producer.send(bytes);
        } catch (PulsarClientException e) {
            fail(e.getMessage());
        }
    }

}
