var kafka = require('kafka-node');

class Base {

    /**
     * constructor
     * @param {*} groupId 
     */
    constructor(client) {
        this.client = client;
        this.messageQueue = [];
        this.processing = false;
    }
    
    /**
     * initConsumerGroup
     * @param {*} consumerId 
     * @param {*} topicArray 
     */
	initConsumer(topicArray, groupId) {
        this.consumer = new kafka.Consumer(this.client, topicArray, {
            autoCommit: false,
            fromOffset: true,
            groupId: groupId
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
        this.producer = new kafka.Producer(this.client);
        this.producer.on('ready', this.producerReadyCb);
        this.producer.on('error', (err) => {
            this.producerErrorCb(err)
            this.initProducer(this.producerReadyCb, this.producerErrorCb);
        })
    }
}
module.exports = Base;