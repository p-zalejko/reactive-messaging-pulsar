package io.smallrye.reactive.messaging.pulsar.helper;

import static org.assertj.core.api.Assertions.fail;

import java.util.UUID;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class TestDataPublisher {

    public void produceTestMessages(PulsarClient pulsarClient, String topic, int count) throws PulsarClientException {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("data")
                .create();

        for (int i = 0; i < count; i++) {
            try {
                producer.send(UUID.randomUUID().toString().getBytes());
            } catch (PulsarClientException e) {
                fail(e.getMessage());
            }
        }
    }
}
