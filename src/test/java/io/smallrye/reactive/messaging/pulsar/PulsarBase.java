package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.PulsarContainer;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.pulsar.consumer.ConsumerFactory;
import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSourceFactory;
import io.smallrye.reactive.messaging.pulsar.helper.MapBasedConfig;
import io.smallrye.reactive.messaging.pulsar.helper.TestDataPublisher;
import io.smallrye.reactive.messaging.pulsar.producer.ProducerFactory;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSinkFactory;
import io.vertx.reactivex.core.Vertx;

public class PulsarBase {

    @Rule
    public PulsarContainer pulsarContainer = new PulsarContainer();

    Vertx vertx;
    TestDataPublisher testDataPublisher;
    WeldContainer container;
    String pulsarBrokerUrl;
    PulsarClient pulsarClient;

    @Before
    public void setup() throws PulsarClientException {
        vertx = Vertx.vertx();
        testDataPublisher = new TestDataPublisher();

        pulsarBrokerUrl = pulsarContainer.getPulsarBrokerUrl(); // "pulsar://localhost:6650";
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl)
                .build();
    }

    @After
    public void tearDown() throws PulsarClientException {
        vertx.close();
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        pulsarClient.close();
    }

    static Weld baseWeld() {
        Weld weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(PulsarConnector.class);
        weld.disableDiscovery();
        return weld;
    }

    static void addConfig(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }
    }

    <T> T deploy(MapBasedConfig config, Class<T> clazz) {
        Weld weld = baseWeld();
        addConfig(config);
        weld.addBeanClass(clazz);
        weld.addBeanClass(PulsarClientFactory.class);
        weld.addBeanClass(PulsarSourceFactory.class);
        weld.addBeanClass(PulsarSinkFactory.class);
        weld.addBeanClass(ConsumerFactory.class);
        weld.addBeanClass(ProducerFactory.class);
        weld.addBeanClass(PulsarBrokerConfig.class);
        weld.disableDiscovery();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(clazz).get();
    }
}
