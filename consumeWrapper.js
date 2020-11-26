const kafka = require('kafka-node');
const DBController = require("./db");

class ConsumeWrapper {
    
    /**
     * constructor
     * @param {*} client 
     * @param {*} topicArray 
     * @param {*} groupId 
     */
    constructor(client, topicArray, groupId) {
        this.topicArray = topicArray;
        this.groupId = groupId;
        this.client = client;
        this.messageQueue = [];
        this.processing = false;
    }

    /**
     * start
     * @param {*} handleMessage 
     * @param {*} onDbError 
     * @param {*} onConsumerClosed 
     */
    async start(handleMessage, onDbError, onConsumerClosed, remainingBuffer) {
        try {
            for(let i=0; i<this.topicArray.length; i++){
                let result = await DBController.getTopicOffset(this.groupId, this.topicArray[i].topic, this.topicArray[i].partition);
                if(!result) {
                    await DBController.createTopicOffset(this.groupId, this.topicArray[i].topic, this.topicArray[i].partition, 0);
                } else {
                    this.topicArray[i].offset = result.offset;
                }
            }
        } catch (err) {
            return onDbError(err);
        }
        this.messageQueue = remainingBuffer ? remainingBuffer : [];
        this.consumer = new kafka.Consumer(this.client, this.topicArray, {
            autoCommit: false,
            fromOffset: true,
            groupId: this.groupId
        });
        
        this.handleMessage = handleMessage;
        this.onConsumerClosed = onConsumerClosed;
        this.consumer.on('error', this._onError.bind(this));
        this.consumer.on('message', this._onMessage.bind(this));
    }

    _onMessage(message) {
        try {
            let _jm = JSON.parse(message.value);
            message.value = _jm;
        } catch (error) {}
        this.messageQueue.push(message);
        this.processQueue();
    }

    _onError(err) {
        if(this.consumer) {
            this.consumer.close(false, (err) => {
                this.cleanup();
                this.onConsumerClosed(err);
            });
        } else {
            this.onError(err);
        }
    }

    cleanup() {
        try {
            this.consumer.removeListener('message', this._onMessage);
            this.consumer.removeListener('error', this._onError);
        } catch (error) {}
    }

    /**
     * processQueue
     */
    processQueue() {
        if(!this.processing && this.messageQueue.length > 0) {
            this.processing = true;
            let nextMessage = this.messageQueue.shift();
            this.handleMessage(nextMessage).then(async function (_message) {
                await DBController.updateTopicOffset(this.groupId, _message.topic, _message.partition, _message.offset + 1);
                this.processing = false;
                this.processQueue();
            }.bind(this, nextMessage)).catch(error => {
                if(process.env.LOG_MSG_PROCESSING_ERRORS)
                    console.log(error);
                this.messageQueue.unshift(nextMessage);
                this.processing = false;
                this._onError(error);
            });
        }
    }
}
module.exports = ConsumeWrapper;