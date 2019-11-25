package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.pulsar.consumer.ConsumerFactory;
import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSourceFactory;
import io.smallrye.reactive.messaging.pulsar.helper.MapBasedConfig;
import io.smallrye.reactive.messaging.pulsar.producer.ProducerFactory;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSinkFactory;

public class EndToEndPulsarTest extends PulsarBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void test() throws PulsarClientException {
        String pulsarBrokerUrl = pulsarContainer.getPulsarBrokerUrl(); // "pulsar://localhost:6650";
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl)
                .build();

        ConsumptionBean bean = deploy(getConfig(pulsarBrokerUrl));

        assertThat(bean.getDataTopicMessages()).isEmpty();
        assertThat(bean.getSinkTopicMessages()).isEmpty();

        int count = 10;
        testDataPublisher.produceTestMessages(pulsarClient, "data", count);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getDataTopicMessages().size() >= count);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getSinkTopicMessages().size() >= count);
    }

    private MapBasedConfig getConfig(String pulsarUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put("pulsar.servers", pulsarUrl);

        String sourcePrefix = "mp.messaging.incoming.data.";
        config.put(sourcePrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sourcePrefix + "topic", "data");
        config.put(sourcePrefix + "topic-subscription", "foo");

        String sinkOutPrefix = "mp.messaging.outgoing.sinkOut.";
        config.put(sinkOutPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkOutPrefix + "topic", "sinkTopic");

        String sinkInPrefix = "mp.messaging.incoming.sinkIn.";
        config.put(sinkInPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
        config.put(sinkInPrefix + "topic", "sinkTopic");
        config.put(sinkInPrefix + "topic-subscription", "foo");

        return new MapBasedConfig(config);
    }

    private ConsumptionBean deploy(MapBasedConfig config) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(PulsarClientFactory.class);
        weld.addBeanClass(PulsarSourceFactory.class);
        weld.addBeanClass(PulsarSinkFactory.class);
        weld.addBeanClass(ConsumerFactory.class);
        weld.addBeanClass(ProducerFactory.class);
        weld.addBeanClass(PulsarBrokerConfig.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

}
