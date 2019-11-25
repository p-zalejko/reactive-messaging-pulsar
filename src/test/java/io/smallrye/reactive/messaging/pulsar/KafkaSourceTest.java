package io.smallrye.reactive.messaging.pulsar;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.pulsar.consumer.ConsumerFactory;
import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSource;
import io.smallrye.reactive.messaging.pulsar.consumer.PulsarSourceFactory;
import io.smallrye.reactive.messaging.pulsar.producer.ProducerFactory;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSink;
import io.smallrye.reactive.messaging.pulsar.producer.PulsarSinkFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class KafkaSourceTest extends KafkaTestBase {

  private WeldContainer container;
  private PulsarSourceFactory pulsarSourceFactory;
//  @Rule
//  public PulsarContainer pulsarContainer = new PulsarContainer();

  @Before
  public void beforeTest() {
    pulsarSourceFactory = new PulsarSourceFactory(new ConsumerFactory());
  }

  @After
  public void cleanup() {
    if (container != null) {
      container.close();
    }
    // Release the config objects
    SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
  }

  @Test
  public void testSource() throws PulsarClientException {
    String pulsarBrokerUrl = "pulsar://localhost:6650";//pulsarContainer.getPulsarBrokerUrl();
    PulsarClient pulsarClient = PulsarClient.builder()
      .serviceUrl(pulsarBrokerUrl)
      .build();

    String topic = UUID.randomUUID().toString();
    Map<String, Object> config = newCommonConfig();
    config.put("topic", topic);
    config.put("topic-subscription", "foo");
//    config.put("value.deserializer", IntegerDeserializer.class.getName());
    PulsarSource source = pulsarSourceFactory.create(new MapBasedConfig(config), pulsarClient);

    List<Message<?>> messages = new ArrayList<>();
    source.getSource().forEach(messages::add).run();

    Producer<byte[]> producer = pulsarClient.newProducer()
      .topic(topic)
      .create();

//    AtomicInteger counter = new AtomicInteger();
    produceTestMessages(producer);

    await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
//    assertThat(messages.stream().map(m -> ((Message<String, Integer>) m).getPayload())
//      .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }


  //
  private Map<String, Object> newCommonConfig() {
    String randomId = UUID.randomUUID().toString();
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("group.id", randomId);
//    config.put("key.deserializer", StringDeserializer.class.getName());
    config.put("enable.auto.commit", "false");
    config.put("auto.offset.reset", "earliest");
    return config;
  }

  //
  private MapBasedConfig myKafkaSourceConfig() {
    String sourcePrefix = "mp.messaging.incoming.data.";
    Map<String, Object> config = new HashMap<>();
    config.put(sourcePrefix + "connector", PulsarConnector.CONNECTOR_NAME);
    config.put(sourcePrefix + "topic", "data");
    config.put(sourcePrefix + "topic-subscription", "foo");

    String sinkOutPrefix = "mp.messaging.outgoing.sink.";
    config.put(sinkOutPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
    config.put(sinkOutPrefix + "topic", "sink1");
//
    String sinkInPrefix = "mp.messaging.incoming.sinka.";
    config.put(sinkInPrefix + "connector", PulsarConnector.CONNECTOR_NAME);
    config.put(sinkInPrefix + "topic", "sink1");
    config.put(sinkInPrefix +"topic-subscription", "foo2");

    return new MapBasedConfig(config);
  }

  //
  @Test
  public void testABeanConsumingTheKafkaMessages() throws PulsarClientException {
    String pulsarBrokerUrl = "pulsar://localhost:6650";//pulsarContainer.getPulsarBrokerUrl();
    PulsarClient pulsarClient = PulsarClient.builder()
      .serviceUrl(pulsarBrokerUrl)
      .build();

    ConsumptionBean bean = deploy(myKafkaSourceConfig());
    List<Object> list = bean.getResults();
    List<Message<Object>> kafkaMessages = bean.getKafkaMessages();
    assertThat(list).isEmpty();
    assertThat(kafkaMessages).isEmpty();

    Producer<byte[]> producer = pulsarClient.newProducer()
      .topic("data")
      .create();

    produceTestMessages(producer);
//
    await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
    await().atMost(2, TimeUnit.MINUTES).until(() -> kafkaMessages.size() >= 10);
  }

  private void produceTestMessages(Producer<byte[]> producer) {
    for (int i = 0; i < 10; i++) {
      try {
        MessageId send = producer.send("fpp".getBytes());
      } catch (PulsarClientException e) {
        e.printStackTrace();
      }
    }
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
    weld.addBeanClass(PulsarConfig.class);
    weld.disableDiscovery();
    container = weld.initialize();
    return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
  }

}
