# kafka-controller

A NPM Kafka wrapper arount the "kafka-node" module, dummy proof adding reconnect management and consumer strict order processing for long running tasks.
The module will also buffer producer messages in case the broker is down, and submit them automatically when the broler is back up.

## Installation

```
npm i kafka-controller
```

In case of some lib errors, you need to change the file `./node_modules/kafka-node/lib/kafkaClient.js`, look for function `KafkaClient.prototype.loadMetadataForTopics` and change it like the following:

```
try {
    const broker = this.brokerForLeader();
    const correlationId = this.nextId();
    const supportedCoders = getSupportedForRequestType(broker, 'metadata');
    const request = supportedCoders.encoder(this.clientId, correlationId, topics);

    this.queueCallback(broker.socket, correlationId, [supportedCoders.decoder, cb]);
    broker.write(request);
} catch (err) {
  callback(err);
}  
```

## Usage

Running long task processing for kafka events can be tricky, Kafka has a session timeout which will result in events being re-consumed.  
kafka-controller solves this by persiting the topic partition offsets in a database, and handeling offsets on the client side. This is why you will need a Postgres Database to use this module.

The following environement variables need to be set:

- KAFKA_HOST
- KAFKA_PORT
- DB_HOST
- DB_USER
- DB_PASS
- DB_PORT
- DB_KAFKA_NAME

### Andmin stuff

```javascript
const KafkaController = require("kafka-controller");
const kafka = new KafkaController();

kafka.initAdmin(async () => {
    await kafka.admin.createTopic("foo-topic", 1);

    await kafka.admin.createPartitions("foo-topic", 2);
    
    let topicDetails = await kafka.admin.getTopicDetails("foo-topic");
    await kafka.admin.deleteTopic("foo-topic");

    let topics = await kafka.admin.listTopics();
}, (err) => {
    console.log("could not connect to Kafka");
});
```

### Consumer & producers

```javascript
const KafkaController = require("kafka-controller");
const kafka = new KafkaController();

let processMessage = async (message) => {
    console.log(message);
}
kafka.registerConsumer("my-consumer-group", "apaas-bot-registry", 0, processMessage);
kafka.produceMessage("apaas-bot-registry", 0, "fookey", {"foo": "bar", "bar": 1})
```
