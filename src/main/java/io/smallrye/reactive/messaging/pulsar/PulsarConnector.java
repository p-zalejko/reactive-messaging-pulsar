package io.smallrye.reactive.messaging.pulsar;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSource;
import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSourceFactory;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSink;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSinkFactory;
import io.vertx.reactivex.core.Vertx;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnector.class);

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    @Inject
    private Instance<Vertx> instanceOfVertx;
    @Inject
    private PulsarClientFactory pulsarClientFactory;
    @Inject
    private PulsarBrokerConfig pulsarConfig;
    @Inject
    private PulsarSourceFactory pulsarSourceFactory;
    @Inject
    private PulsarSinkFactory pulsarSinkFactory;

    private List<PulsarSource> sources = new CopyOnWriteArrayList<>();
    private List<PulsarSink> sinks = new CopyOnWriteArrayList<>();

    private boolean internalVertxInstance = false;
    private Vertx vertx;
    private PulsarClient client;

    public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        sources.forEach(PulsarSource::closeQuietly);
        sinks.forEach(PulsarSink::closeQuietly);

        if (internalVertxInstance) {
            vertx.close();
        }

        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @PostConstruct
    void init() {
        if (instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }

        try {
            client = pulsarClientFactory.createClient(pulsarConfig);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        try {
            PulsarSource source = pulsarSourceFactory.create(new PulsarChannelConfig(config), client);
            sources.add(source);
            return source.getSource();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Could not create Apache Pulsar source", e);
        }
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        try {
            PulsarSink sink = pulsarSinkFactory.create(new PulsarChannelConfig(config), client);
            sinks.add(sink);
            return sink.getSink();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Could not create Apache Pulsar sink", e);
        }
    }

}
