# kafka-controller

A NPM Kafka wrapper arount the "kafka-node" module, dummy proof adding reconnect management and consumer strict order processing for long running tasks.
The module will also buffer producer messages in case the broker is down, and submit them automatically when the broler is back up.

## Installation

```
npm i kafka-controller
```

<!-- In case of some lib errors, you need to change the file `./node_modules/kafka-node/lib/kafkaClient.js`, look for function `KafkaClient.prototype.loadMetadataForTopics` and change it like the following:

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
``` -->

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

<!-- ### Andmin stuff

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
``` -->

### To subscribe to a topic

```javascript
const KafkaController = require("/home/node/kafka-controller");

// Init Kafka controller
const kafka = new KafkaController();

kafka.registerConsumer("<a unique client consumer group name>", [
    {
        "topic": "<target topic>",
        "partition": 0
    }
], async (message) => {
    // NOTE: If you throw an exception / error here, then this message will get consumed again untill it executes without errors.
});
```

### To search for topic events

```javascript
const KafkaController = require("/home/node/kafka-controller");

// Init Kafka controller
const kafka = new KafkaController();

let reqRespController = kafka.getReqResController();
reqRespController.init(kafka, "<a unique client consumer group name>");

// Search kafka, return n number of events starting from specified partition offset
let events = await kafka.searchEvents("<kafkaTopic>", "<kafkaPartition>", "<task.kafkaOffset>", {
    eventCount: 1 
});

// Search kafka, filter events using "jsonata" expressions (https://www.npmjs.com/package/jsonata)
let events = await kafka.searchEvents("<kafkaTopic>", "<kafkaPartition>", "<task.kafkaOffset>", {
    filters: [{
        "expression": "data.solarcell.job.uid",
        "value": 14
    }],
    break: []
});
// The "break" array is similar to the "filters" array, but it will tell the search client to stop searching once those conditions are found
```

### Produce messages

```javascript
const KafkaController = require("/home/node/kafka-controller");

// Init Kafka controller
const kafka = new KafkaController();

// Produce a message and return the message offset index
//
// The message offset index can then be attached to DB objects such as a solar cell object in DynamoDB. 
// This way, you can search Kafka later on to find messages related to a specific solar cell 
// event (ex. solar cell unit that goes through the various microservice stages). 
// You can still search kafka without it by starting at offset 0, but this might become a performance 
// bottleneck if the Kafka event queue becomes very large.
let messageOffset = await this.controlPlane.kafka.produceMessage("<kafkaTopic>", "<kafkaPartition>", "0", {
    "foo": bar
}, true); // if true, the call will wait for a successfull publish and return the offset index

// Produce a message asyncroniously, without waiting for the offset
await this.controlPlane.kafka.produceMessage("<kafkaTopic>", 0, "<kafkaPartition>", {
    "foo": bar
});
```