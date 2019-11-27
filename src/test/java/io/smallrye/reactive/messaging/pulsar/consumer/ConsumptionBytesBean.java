package io.smallrye.reactive.messaging.pulsar.consumer;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ConsumptionBytesBean extends AbstractConsumptionBean<byte[], byte[]> {

    public ConsumptionBytesBean() {
        super(bytes -> bytes);
    }
}
