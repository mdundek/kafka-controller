const async = require('async');
const kafka = require('kafka-node');

const KafkaConsumeOrdered = require("./consumeOrdered");
const KafkaProducer = require("./producer");
const KafkaAdmin = require("./admin");
const DBController = require("./db");
DBController.init();

let _envDependencies = ["KAFKA_HOST", "KAFKA_PORT", "DB_HOST", "DB_PORT", "DB_KAFKA_NAME", "DB_USER", "DB_PASS"];

class KafkaController {
    
    /**
     * constructor
     */
    constructor() {
        for(let _dep of _envDependencies){
            if(!process.env[_dep]) {
                throw new Error("kafka-controller requires that the following environement variables are set: " + _envDependencies.join(", "));
            }
        }
        
        this.consumers = [];
        this.bufferProducerMessages = [];
        this.producerBufferBussy = false;
        this.cleanup = false;

        this.client = new kafka.KafkaClient({ 
            kafkaHost: `${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}`
        });

        //do something when app is closing
        this._cleanedup = false;
        process.on('exit', this._onExit.bind(this,{cleanup:true}));
        process.on('SIGINT', this._onExit.bind(this, {exit:true}));
        process.on('SIGUSR1', this._onExit.bind(this, {exit:true}));
        process.on('SIGUSR2', this._onExit.bind(this, {exit:true}));
        process.on('uncaughtException', this._onExit.bind(this, {exit:true}));
    }

    /**
     * initProducer
     */
    initProducer() {
        this.producerReady = false;
        this.producer = new KafkaProducer(this.client);
        this.producer.initProducer(() => {
            this.producerReady = true;
            this._processProducerQueue();
        }, (err) => {
            this.producerReady = false;
        });
    }

    /**
     * initAdmin
     */
    initAdmin(success, fail) {
        this.admin = new KafkaAdmin(success, fail);
    }

    /**
     * _processProducerQueue
     */
    _processProducerQueue() {
        if(!this.producerBufferBussy && this.bufferProducerMessages.length > 0) {
            this.producerBufferBussy = true;
            let payload = this.bufferProducerMessages.shift();
            this.producer.send([payload], function (_payload, err, data) {
                if(err) {
                    this.bufferProducerMessages.unshift(_payload);
                    this.producerBufferBussy = false;
                    this.initProducer();
                } else {
                    this.producerBufferBussy = false;
                    this._processProducerQueue();
                }
            }.bind(this, payload));
        }
    }

    /**
     * _onExit
     * @param {*} options 
     * @param {*} exitCode 
     */
    _onExit (options, exitCode) {
        if(!this.cleanedup){
            this.cleanedup = true;
            async.each(this.consumers.map(c => c.consumer), (_consumer, callback) => {
                if(_consumer)
                    _consumer.close(false, callback);
                else
                    callback();
            }, () => {
                if (options.cleanup) console.log('clean');
                if (exitCode || exitCode === 0) console.log(exitCode);
                if (options.exit) process.exit();
            });
        }
    }

    /**
     * 
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} processMessageCb 
     */
    registerConsumer (groupId, topic, partition, processMessageCb) {
        let kafkaConsumer = new KafkaConsumeOrdered(this.client, [{topic: topic, partition: partition}], groupId);
        kafkaConsumer.start(processMessageCb);
        this.consumers.push(kafkaConsumer);
    }

    /**
     * produceMessage
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} key 
     * @param {*} message 
     */
    produceMessage (topic, partition, key, message) {
        if(this.producerReady){
            let payload = {
                topic: topic,
                messages: typeof message == 'string' ? message : JSON.stringify(message),
                key: key,
                partition: partition != null ? partition : 0
            };
            this.producer.send([payload], function (_payload, err) {
                if(err) {
                    this.bufferProducerMessages.push(_payload);
                    this.initProducer();
                }
            }.bind(this, payload));
        } else {
            this.bufferProducerMessages.push({
                topic: topic,
                key: key,
                messages: typeof message == 'string' ? message : JSON.stringify(message),
                partition: partition != null ? partition : 0
            });
        }
    }
}

module.exports = KafkaController;