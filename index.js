const async = require('async');
const kafka = require('kafka-node');
const shortid = require('shortid');
const jsonata = require("jsonata");

const KafkaSearchConsumerWrapper = require("./searchConsumerWrapper");
const KafkaConsumeWrapper = require("./consumeWrapper");
const KafkaProducerWrapper = require("./producerWrapper");
const KafkaAdminWrapper = require("./adminWrapper");
const DBController = require("./db");
const RequestResponseHelper = require("./requestResponse");

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
        this.producerReady = false;

        this._initProducer();
        this._initSearchConsumer();

        //do something when app is closing
        this._cleanedup = false;
        process.on('exit', this._onExit.bind(this,{cleanup:true}));
        process.on('SIGINT', this._onExit.bind(this, {exit:true}));
        process.on('SIGUSR1', this._onExit.bind(this, {exit:true}));
        process.on('SIGUSR2', this._onExit.bind(this, {exit:true}));
        process.on('uncaughtException', this._onExit.bind(this, {exit:true}));
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
                if (exitCode || exitCode === 0) console.log("Exit code", exitCode);
                if (options.exit) process.exit();
            });
        }
    }

    /**
     * initAdmin
     */
    initAdmin(success, fail) {
        this.admin = new KafkaAdminWrapper(success, fail);
    }

    /**
     * getReqResController
     */
    getReqResController() {
        return RequestResponseHelper;
    }

         /**
     * _createClient
     */
    _createClient() {
        if(process.env.KAFKA_CONNECTION_MODE && process.env.KAFKA_CONNECTION_MODE == "internal") {
            return new kafka.KafkaClient({ 
                kafkaHost: `${process.env.KAFKA_HOST}:${process.env.KAFKA_INTERNAL_PORT}`
            });
        } else {
            return new kafka.KafkaClient({ 
                kafkaHost: `${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}`
            });
        }
    }


    /**************************************************************************************
     *                                      CONSUMER
     **************************************************************************************/

    /**
     * 
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} processMessageCb 
     */
    async registerConsumer (groupId, topics, processMessageCb, remainingBuffer) {
        let client = this._createClient();
        let uid = shortid.generate();
        let regData = {
            "uid": uid,
            "groupId": groupId,
            "topics": topics,
            "processMessageCb": processMessageCb,
            "client": client,
            "remainingBuffer": remainingBuffer
        };
        client.once('ready', this._consumerClientOnReady.bind(this, regData));
        client.once('close', this._consumerClientOnClose.bind(this, regData));
        client.once('error', this._consumerClientOnClose.bind(this, regData));
    }

    /**
     * _consumerClientOnReady
     */
    async _consumerClientOnReady(regData) {
        console.log("Consumer client ready", regData.uid);

        let kafkaConsumer = new KafkaConsumeWrapper(regData.client, regData.topics, regData.groupId);
        kafkaConsumer.uid = regData.uid;

        // console.log(topics);
        await kafkaConsumer.start(
            regData.processMessageCb, 
            this._consumerClientOnClose.bind(this, regData), 
            this._consumerClientOnClose.bind(this, regData),
            regData.remainingBuffer
        );

        this.consumers.push(kafkaConsumer);
    }

    /**
     * _consumerClientOnClose
     */
    _consumerClientOnClose(regData, error, messageBuffer) {
        console.log("Consumer client close", regData.uid);
        // console.log(messageBuffer);
        let remainingBuffer = null;
        for(let i=0; i<this.consumers.length; i++) {
            if(this.consumers[i].uid == regData.uid) {
                // console.log(`Consumer client close:`, regData.uid); 
                remainingBuffer = JSON.parse(JSON.stringify(this.consumers[i].messageQueue));

                this.consumers[i].cleanup();
                if(regData.client){
                    regData.client.removeListener('ready', this._consumerClientOnReady);
                    regData.client.removeListener('close', this._consumerClientOnClose);
                    regData.client.removeListener('error', this._consumerClientOnClose);
                }
                i = this.consumers.length;
            }
        }
        regData.client.close();
        this._removeConsumer(regData);
        setTimeout(function (_regData, _remainingBuffer) {
            // console.log("Attempting reconnect...");
            this.registerConsumer(_regData.groupId, _regData.topics, _regData.processMessageCb, _remainingBuffer);
        }.bind(this, regData, remainingBuffer ? remainingBuffer : regData.remainingBuffer), 4000);
    }

    /**
     * _removeConsumer
     * @param {*} regData 
     */
    _removeConsumer(regData) {
        for(let i=0; i<this.consumers.length; i++) {
            if(this.consumers[i].uid == regData.uid) {
                this.consumers.splice(i, 1);
                i--;
            }
        }
    }


    /**************************************************************************************
     *                                      PRODUCER
     **************************************************************************************/

    /**
     * _initProducer
     */
    _initProducer(remainingBuffer) {
        try {
            this.producerClient = this._createClient();
            // let uid = shortid.generate();

            this.producerClient.once('ready', this._producerClientOnReady.bind(this, remainingBuffer));
            this.producerClient.on('close', this._producerClientOnClose.bind(this, remainingBuffer));
            this.producerClient.on('error', this._producerClientOnClose.bind(this, remainingBuffer));
        } catch (error) {
            setTimeout(function (_remainingBuffer) {
                this._initProducer(_remainingBuffer);
            }.bind(this, remainingBuffer), 4000);
        }
    }

    /**
     * _producerClientOnReady
     */
    _producerClientOnReady(remainingBuffer) {
        // console.log("Producer client ready");
        this.producer = new KafkaProducerWrapper(this.producerClient);
        this.producer.initProducer(
            () => {
                this.producerReady = true;
                this._processProducerQueue();
            }, 
            this._producerClientOnClose.bind(this),
            remainingBuffer
        );
    }

    /**
     * _producerClientOnClose
     */
    _producerClientOnClose(remainingBuffer) {
        // console.log("Producer client close");
        this.producerReady = false;
        if(this.producer)
            this.producer.cleanup();
        this.producer = null;
        if(this.producerClient){
            this.producerClient.removeListener('ready', this._producerClientOnReady);
            this.producerClient.removeListener('close', this._producerClientOnClose);
            this.producerClient.removeListener('error', this._producerClientOnClose);
            remainingBuffer = this.producer ? JSON.parse(JSON.stringify(this.producer.messageQueue)) : null;
            this.producerClient.close();
        }
        setTimeout(function (_remainingBuffer) {
            this._initProducer(_remainingBuffer);
        }.bind(this, remainingBuffer), 4000);
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
                    this._producerClientOnClose();
                } else {
                    this.producerBufferBussy = false;
                    this._processProducerQueue();
                }
            }.bind(this, payload));
        }
    }

    /**
     * produceMessage
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} key 
     * @param {*} message 
     */
    produceMessage (topic, partition, key, message, strict) {
        return new Promise((resolve, reject) => {
            if(this.producerReady){
                let payload = {
                    topic: topic,
                    messages: typeof message == 'string' ? message : JSON.stringify(message),
                    key: key,
                    partition: partition != null ? partition : 0
                };
                this.producer.send([payload], function (_payload, err, data) {
                    if(err) {
                        reject(err);
                    } else {
                        resolve(data[topic][key]);
                    }
                }.bind(this, payload));
            } else if(!strict) {
                this.bufferProducerMessages.push({
                    topic: topic,
                    key: key,
                    messages: typeof message == 'string' ? message : JSON.stringify(message),
                    partition: partition != null ? partition : 0
                });
                resolve();
            } else {
                reject();
            }
        });
    }


    /**************************************************************************************
     *                                      SEARCH
     **************************************************************************************/

    /**
     * _initSearchConsumer
     */
    _initSearchConsumer() {
        try {
            this.searchClient = this._createClient();

            this.searchClient.once('ready', this._searchClientOnReady.bind(this));
            this.searchClient.on('close', this._searchClientOnClose.bind(this));
            this.searchClient.on('error', this._searchClientOnClose.bind(this));
        } catch (error) {
            setTimeout(function () {
                this._initSearchConsumer();
            }.bind(this), 4000);
        }
    } 

    /**
     * _searchClientOnReady
     */
    _searchClientOnReady() {
        // console.log("Search consumer client ready");
        this.searchConsumer = new KafkaSearchConsumerWrapper(this.searchClient, function (err) {
            this._searchClientOnClose();
        
        }.bind(this));
        this.searchConsumer.consumer.pause();
        this.searchClientReady = true;
    }

    /**
     * _searchClientOnClose
     */
    _searchClientOnClose() {
        this.searchClientReady = false;
        // console.log("Search consumer client close");

        if(this.searchConsumer)
            this.searchConsumer.cleanup();
        this.searchConsumer = null;

        if(this.searchClient){
            this.searchClient.removeListener('ready', this._searchClientOnReady);
            this.searchClient.removeListener('close', this._searchClientOnClose);
            this.searchClient.removeListener('error', this._searchClientOnClose);
            this.searchClient.close();
        }
        setTimeout(function () {
            this._initSearchConsumer();
        }.bind(this), 4000);
    }

    /**
     * searchEvents
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} offset 
     * @param {*} params 
     */
    searchEvents (topic, partition, offset, params) {
        return new Promise((resolve, reject) => {
            if(!this.searchClientReady){
                return reject("Kafka not available");
            }

            var BreakException = {};
            let filterKeys = params && params.filters ? params.filters.map(o => {
                o.expression = jsonata(o.expression);
                return o;
            }) : null;

            let breakKeys = params && params.break ? params.break.map(o => {
                o.expression = jsonata(o.expression);
                return o;
            }) : null;

            let events = [];

            let checkIfFilterMatch = (event) => {
                if(filterKeys) {
                    try {
                        let _value = JSON.parse(event.value);
                        filterKeys.forEach((o) => {
                            let r = o.expression.evaluate(_value);
                            if(r == undefined || r != o.value) {
                                throw BreakException;
                            }
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                } else {
                    return true;
                }
            }

            let checkIfBreak = (event) => {
                if(breakKeys) {
                    try {
                        let _value = JSON.parse(event.value);
                        breakKeys.forEach((o) => {
                            let r = o.expression.evaluate(_value);
                            if(r == undefined || r != o.value) {
                                throw BreakException;
                            }
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                }
                else if(params.eventCount != undefined && params.eventCount != null){
                   return events.length >= params.eventCount
                }
                else {
                    return false;
                }
            }

            let performSearch = () => {
                let ignore = false;
                this.searchConsumer.onMessageCallback = (event) => {
                    if(!ignore){
                        if(checkIfFilterMatch(event)) {
                            events.push(event);
                        }
                        if(checkIfBreak(event) || event.offset == (event.highWaterOffset-1)) {
                            ignore = true;
                            this.searchConsumer.consumer.pause();
                            resolve(events.map(_e => {
                                _e.value = JSON.parse(_e.value);
                                return _e;
                            }));
                        }
                    }
                }
                
                this.searchConsumer.onOffsetOutOfRangeCallback = (err) => {
                    console.log("offset out of range", err);
                    this.searchConsumer.consumer.pause();
                    reject(err);
                }
                this.searchConsumer.consumer.resume();
            };

            if(!this.searchConsumer.registeredTopics.find(t => t.topic == topic && t.partition == partition)) {
                this.searchConsumer.consumer.addTopics([{ topic: topic, partition: partition, offset: offset }], (err, added) => {
                    this.searchConsumer.registeredTopics.push({ topic: topic, partition: partition });
                    performSearch();
                }, true);
            } else {
                this.searchConsumer.consumer.setOffset(topic, partition, offset);
                performSearch();
            }
        });
    }
}

module.exports = KafkaController;