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

    /**
     * send
     * @param {*} payload 
     * @param {*} callback 
     */
    send(payload, callback) {
        this.producer.send(payload, callback);
    }
}
module.exports = ProducerWrapper;