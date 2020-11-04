const Base = require("./base");
const DBController = require("./db");

class ConsumeExactlyOnce extends Base {
    
    /**
     * constructor
     * @param {*} consumerId 
     * @param {*} topicArray 
     */
    constructor(client, topicArray, groupId) {
        super(client);
        this.topicArray = topicArray;
        this.groupId = groupId;
    }

    /**
     * start
     * @param {*} handleMessage 
     * @param {*} handleError 
     */
    start(handleMessage) {
        (async() => {
            for(let i=0; i<this.topicArray.length; i++){
                let result = await DBController.getTopicOffset(this.groupId, this.topicArray[i].topic, this.topicArray[i].partition);
                if(!result) {
                    await DBController.createTopicOffset(this.groupId, this.topicArray[i].topic, this.topicArray[i].partition, 0);
                } else {
                    this.topicArray[i].offset = result.offset;
                }
            }
            this.initConsumer(this.topicArray, this.groupId);
            this.handleMessage = handleMessage;
            this.consumer.on('error', () => {
                this.consumer.close(false, () => {
                    this.start(this.handleMessage);
                });
            });
            this.consumer.on('message', (message) => {
                try {
                    let _jm = JSON.parse(message.value);
                    message.value = _jm;
                } catch (error) {}
                this.messageQueue.push(message);
                this.processQueue();
            });
        })();
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
                this.messageQueue.unshift(nextMessage);
                this.processing = false;
                this.processQueue();
            });
        }
    }
}
module.exports = ConsumeExactlyOnce;