package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.junit.Test;

import io.smallrye.reactive.messaging.pulsar.consumer.ConsumptionBytesBean;
import io.smallrye.reactive.messaging.pulsar.consumer.ConsumptionBytesToStringBean;
import io.smallrye.reactive.messaging.pulsar.helper.ConfigHelper;
import io.smallrye.reactive.messaging.pulsar.helper.MapBasedConfig;

public class EndToEndPulsarTest extends PulsarBase {

    @Test
    public void test_defaultSchema() throws PulsarClientException {
        MapBasedConfig config = ConfigHelper.getDefaultConfig(pulsarBrokerUrl);
        ConsumptionBytesBean bean = deploy(config, ConsumptionBytesBean.class);
        assertThat(bean.getDataTopicMessages()).isEmpty();
        assertThat(bean.getSinkTopicMessages()).isEmpty();

        int count = 10;
        testDataPublisher.produceTestMessages(pulsarClient, "data", count);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getDataTopicMessages().size() >= count);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getSinkTopicMessages().size() >= count);
    }

    @Test
    public void test_bytesAndStringSchema() throws PulsarClientException {
        MapBasedConfig config = ConfigHelper.getConfig(pulsarBrokerUrl, BytesSchema.class, StringSchema.class);
        ConsumptionBytesToStringBean bean = deploy(config, ConsumptionBytesToStringBean.class);
        assertThat(bean.getDataTopicMessages()).isEmpty();
        assertThat(bean.getSinkTopicMessages()).isEmpty();

        int count = 10;
        testDataPublisher.produceTestMessages(pulsarClient, "data", count);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getDataTopicMessages().size() >= count);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getSinkTopicMessages().size() >= count);
    }
}
