var kafka = require('kafka-node');

class Base {

    /**
     * constructor
     * @param {*} groupId 
     */
    constructor(groupId) {
        this.messageQueue = [];
        if(groupId) this.groupId = groupId;
        this.processing = false;

        this.consumerGroupOptions = {
            kafkaHost: `${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}`,
            autoCommit: false,
            groupId: this.groupId ? this.groupId : null,
        };
    }
    
    /**
     * initConsumerGroup
     * @param {*} consumerId 
     * @param {*} topicArray 
     */
	initConsumerGroup(consumerId, topicArray) {
        this.consumerGroup = new kafka.ConsumerGroup(Object.assign({ id: consumerId }, this.consumerGroupOptions), topicArray);
        this.consumerGroup.on('connect', () => {
            console.log(`Consumer group "${this.groupId}" connected`);
        });
    }

    /**
     * initConsumerGroup
     * @param {*} consumerId 
     * @param {*} topicArray 
     */
	initConsumer(topicArray) {
        this.client = new kafka.KafkaClient({ 
            kafkaHost: this.consumerGroupOptions.kafkaHost
        });
        this.consumer = new kafka.Consumer(this.client, topicArray, {
            autoCommit: false,
            fromOffset: true
        });
        this.consumer.on('connect', () => {
            console.log(`Consumer connected`);
        });
    }

    /**
     * initProducer
     */
    initProducer(onReady, onError) {
        this.producerReadyCb = onReady;
        this.producerErrorCb = onError;
        this.client = new kafka.KafkaClient({ 
            kafkaHost: this.consumerGroupOptions.kafkaHost 
        });
        this.producer = new kafka.Producer(this.client);
        this.producer.on('ready', this.producerReadyCb);
        this.producer.on('error', (err) => {
            this.producerErrorCb(err)
            this.initProducer(this.producerReadyCb, this.producerErrorCb);
        })
    }
}
module.exports = Base;