package io.smallrye.reactive.messaging.pulsar.consumer;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ConsumptionBytesToStringBean extends AbstractConsumptionBean<byte[], String> {

    public ConsumptionBytesToStringBean() {
        super(bytes -> "hello_" + bytes[0]);
    }
}
