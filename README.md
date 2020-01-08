# Reactive messaging pulsar

A PoC implementation of the reactive messaging specification for Apache Pulsar. The implementation depends on SmallRye Reactive Messaging.

See:
- https://github.com/eclipse/microprofile-reactive-messaging
- https://smallrye.io/smallrye-reactive-messaging/
- https://pulsar.apache.org/


# Example
## Configure broker and topics

```
# URL to the broker
pulsar.servers=pulsar://localhost:6650

# In topic
mp.messaging.incoming.data-in.connector=smallrye-pulsar
mp.messaging.incoming.data-in.topic=sample-topic
mp.messaging.incoming.data-in.topic-subscription=foo
mp.messaging.incoming.data-in.schema=org.apache.pulsar.client.impl.schema.IntSchema

#Out topic
mp.messaging.outgoing.data-out.connector=smallrye-pulsar
mp.messaging.outgoing.data-out.topic=sample-topic
mp.messaging.outgoing.data-out.schema=org.apache.pulsar.client.impl.schema.IntSchema
```

## Consumer and producer

Annotation-based example:
```
    @Incoming("dataIn")
    @Outgoing("dataOut")
    public Message process(Message<T> input) {
        T payload = input.getPayload();
         
        return Message.of(...);
    }
```

Emmitter example:
```
    @Inject
    @Channel("dataOut")
    Emitter<String> emitter;

    public void sendMessage(String message) {
        emitter.send(message);
    }
```