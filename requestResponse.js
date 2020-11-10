const shortid = require('shortid');
shortid.characters('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ$@');

class RequestResponseHelper {
    
    /**
     * 
     * @param {*} kafkaController 
     */
    static init(kafkaController, consumerGroup) {
        this.kafkaController = kafkaController;
        this.kafkaController.registerConsumer(consumerGroup, [
            {
                "topic": "apaas-request-response",
                "partition": 0
            }
        ], this._onConsumerMessage.bind(this));
    }

    /**
     * sleep
     * @param {*} duration 
     */
    static sleep(duration) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, duration);
        });
    }

    /**
     * registerRequestListener
     * @param {*} cb 
     */
    static registerRequestListener(cb, serviceTarget) {
        this.requestListener = cb;
        this.serviceTarget = serviceTarget;
    }

    /**
     * expected
     * @param {*} topicSplit 
     * @param {*} payload 
     */
    static async _checkExpected(messageValue) {
        if(messageValue.type == "response" && this.pendingResponses[messageValue.key]){
            clearTimeout(this.pendingResponses[messageValue.key].timeout);
            this.pendingResponses[messageValue.key].resolve({
                "data": messageValue.data
            });
            delete this.pendingResponses[messageValue.key];
        }
    }

    /**
     * _checkIfTargetTask
     * @param {*} messageValue 
     */
    static async _checkIfTargetTask(messageValue) {
        if(messageValue.type == "request" && messageValue.target == this.serviceTarget && messageValue.timeout > (new Date().getTime() / 1000)){
            let responseData = await this.requestListener(messageValue);
            try {
                await this.kafkaController.produceMessage("apaas-request-response", 0, "0", {
                    type: "response",
                    key: messageValue.key,
                    data: responseData
                }, true);
            } catch (error) {
                this.pendingResponses[requestId].reject(new Error("Could not publish the request to Kafka"));
                delete this.pendingResponses[requestId];
            }
        }
    }

    /**
     * kafkaQueryRequestResponse
     * @param {*} targetService 
     * @param {*} taskName 
     * @param {*} payload 
     * @param {*} timeout 
     */
    static queryRequestResponse(targetService, taskName, payload, timeout) {
        return new Promise((resolve, reject) => {
            if(!this.kafkaController.producerReady) {
                return reject(new Error("Could not publish the request to Kafka"));
            }

            let requestId = null;
            while(requestId == null){
                requestId = shortid.generate();
                if(requestId.indexOf("$") != -1 || requestId.indexOf("@") != -1){
                    requestId = null;
                }
            }

            this.pendingResponses[requestId] = {
                "type": "request", 
                "resolve": resolve,
                "reject": reject,
                "timeout": setTimeout(function(requestId) {
                    if(this.pendingResponses[requestId]){
                        this.pendingResponses[requestId].reject(new Error("Request timed out"));
                        delete this.pendingResponses[requestId];
                    }
                }.bind(this, requestId), timeout ? timeout : 3000)
            };

            this.kafkaController.produceMessage("apaas-request-response", 0, "0", {
                type: "request",
                target: targetService,
            	key: requestId,
                task: taskName,
                timeout: (new Date().getTime() / 1000) + (timeout ? timeout : 3000),
            	data: payload
            }, true).then((offset) => {}).catch(function (_requestId, err) {
                this.pendingResponses[_requestId].reject(new Error("Could not publish the request to Kafka"));
                delete this.pendingResponses[_requestId];
            }.bind(this, requestId));
        });
    }

    /**
     * _onConsumerMessage
     * @param {*} message 
     */
    static async _onConsumerMessage(message) {
        try {
            if(message.topic == 'apaas-request-response') {
                await this._checkExpected(message.value);
                await this._checkIfTargetTask(message.value);
            }
        } catch (error) {
            console.log(error);
            await this.sleep(2000); // Avoid kafka from looping over message reconsume to fast
            throw error;
        }
    }
}
RequestResponseHelper.pendingResponses = {};
module.exports = RequestResponseHelper;