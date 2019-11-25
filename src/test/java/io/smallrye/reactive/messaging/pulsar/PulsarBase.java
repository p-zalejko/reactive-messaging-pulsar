package io.smallrye.reactive.messaging.pulsar;

import org.jboss.weld.environment.se.Weld;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.PulsarContainer;

import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.extension.ChannelProducer;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.pulsar.helper.MapBasedConfig;
import io.smallrye.reactive.messaging.pulsar.helper.TestDataPublisher;
import io.vertx.reactivex.core.Vertx;

public class PulsarBase {

    @Rule
    public PulsarContainer pulsarContainer = new PulsarContainer();

    Vertx vertx;
    TestDataPublisher testDataPublisher;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
        testDataPublisher = new TestDataPublisher();
    }

    @After
    public void tearDown() {
        vertx.close();
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
}
