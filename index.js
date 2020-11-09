const async = require('async');
const kafka = require('kafka-node');
const shortid = require('shortid');

const KafkaSearchConsumerWrapper = require("./searchConsumerWrapper");
const KafkaConsumeWrapper = require("./consumeWrapper");
const KafkaProducerWrapper = require("./producerWrapper");
const KafkaAdminWrapper = require("./adminWrapper");
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
     * initAdmin
     */
    initAdmin(success, fail) {
        this.admin = new KafkaAdminWrapper(success, fail);
    }

    /**
     * 
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} processMessageCb 
     */
    async registerConsumer (groupId, topics, processMessageCb) {
        let client = this._createClient();
        let uid = shortid.generate();

        client.on('ready', function(_uid) {
            console.log(`Client ready`, _uid);
        }.bind(this, uid));
        client.on('close', function(regData) {
            console.log(`Client close:`, uid); 
        }.bind(this, uid));
        
        let onDbError = (regData, err) => {
            console.log("DB error");
            this._removeConsumer(regData)
            regData.client.close(() => {
                setTimeout( function (_regData) {
                    console.log("Attempting reconnect...");
                    this.registerConsumer(_regData.groupId, _regData.topics, _regData.processMessageCb);
                }.bind(this, regData), 6000);
            });
        }

        let onConsumerClosed = (regData, err) => {
            console.log("Consumer error");
            this._removeConsumer(regData);
            setTimeout(function (_regData) {
                console.log("Attempting reconnect...");
                this.registerConsumer(_regData.groupId, _regData.topics, _regData.processMessageCb);
            }.bind(this, regData), 6000);
        }

        let kafkaConsumer = new KafkaConsumeWrapper(client, topics, groupId);
        kafkaConsumer.uid = uid;
        await kafkaConsumer.start(processMessageCb, 
            onDbError.bind(this, {
                "uid": uid,
                "groupId": groupId,
                "topics": topics,
                "processMessageCb": processMessageCb,
                "client": client
        }), onConsumerClosed.bind(this, {
                "uid": uid,
                "groupId": groupId,
                "topics": topics,
                "processMessageCb": processMessageCb
        }));

        this.consumers.push(kafkaConsumer);
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
                        this.bufferProducerMessages.push(_payload);
                    }
                    resolve(data[topic][key]);
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
            let filterKeys = params && params.filter ? Object.keys(params.filter) : null;
            let breakKeys = params && params.break ? Object.keys(params.break) : null;
            let events = [];

            let checkIfFilterMatch = (event) => {
                if(filterKeys){
                    try {
                        filterKeys.forEach((f) => {
                            if(typeof params.filter[f] == "string"){
                                if(event.value.indexOf(`"${f}":"${params.filter[f]}"`) == -1) {
                                    throw BreakException;
                                }
                            } else {
                                if(event.value.indexOf(`"${f}":${params.filter[f]}`) == -1) {
                                    throw BreakException;
                                }
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
                if(breakKeys){
                    try {
                        breakKeys.forEach((f) => {
                            if(typeof params.break[f] == "string"){
                                if(event.value.indexOf(`"${f}":"${params.break[f]}"`) == -1) {
                                    throw BreakException;
                                }
                            } else {
                                if(event.value.indexOf(`"${f}":${params.break[f]}`) == -1) {
                                    throw BreakException;
                                }
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
                            resolve(events);
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
     * _initProducer
     */
    _initProducer() {
        console.log("Calling  init producer");
        let client = this._createClient();
        this.producer = new KafkaProducerWrapper(client);
        this.producer.initProducer(
            () => {
                this.producerReady = true;
                this._processProducerQueue();
            }, 
            (err) => {
                this.producerReady = false;
                this.producer.client.close(() => {
                    setTimeout(function () {
                        this._initProducer();
                    }.bind(this), 6000);
                });
            }
        );
    }

    /**
     * _initSearchConsumer
     */
    _initSearchConsumer() {
        this.searchClient = this._createClient();
        let uid = shortid.generate();
        this.searchClient.on('ready', function(_uid) {
            console.log("Search consumer client ready");
            
            this.searchConsumer = new KafkaSearchConsumerWrapper(this.searchClient, function (__uid, err) {
                this.searchClientReady = false;
                console.log("Search consumer error, remove");
                this._removeConsumer({
                    uid: __uid
                });
            }.bind(this, _uid));
            this.searchConsumer.uid = _uid;

            this.searchConsumer.consumer.pause();
            this.consumers.push(this.searchConsumer);
            this.searchClientReady = true;
        }.bind(this, uid));
        this.searchClient.on('close', function(_uid) {
            this.searchClientReady = false;
            this.searchClient.close(() => {
                setTimeout(function () {
                    console.log("Attempting search consumer reconnect...");
                    this._initSearchConsumer();
                }.bind(this), 6000);
            });
            console.log("===> search client close");
        }.bind(this, uid));
        this.searchClient.on('error', function(_uid) {
            this.searchClientReady = false;
            console.log("===> search client error");
        }.bind(this, uid));
    } 
}

module.exports = KafkaController;