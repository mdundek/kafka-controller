var kafka = require('kafka-node');

class ProducerWrapper {
    /**
     * constructor
     */
    constructor(client) {
        this.client = client;
        this.messageQueue = [];
        this.processing = false;
    }

    /**
     * initProducer
     */
    initProducer(onReady, onError, remainingBuffer) {
        this.producerReadyCb = onReady;
        this.producerErrorCb = onError;
        this.messageQueue = remainingBuffer ? remainingBuffer : [];
        this.producer = new kafka.Producer(this.client);
        this.producer.once('ready', this.producerReadyCb);
        this.producer.on('error', this.producerErrorCb)
    }

    /**
     * send
     * @param {*} payload 
     * @param {*} callback 
     */
    send(payload, callback) {
        this.producer.send(payload, callback);
    }

    cleanup() {
        try {
            this.producer.removeListener('ready', this.producerReadyCb);
            this.producer.removeListener('error', this.producerErrorCb);
        } catch (error) {}
    }
}
module.exports = ProducerWrapper;