const kafka = require('kafka-node');
class SearchConsumer {
    
    /**
     * constructor
     */
    constructor(client, onError) {
        this.onError = onError;
        this.registeredTopics = [];
        this.onMessageCallback = null;
        this.onConsumerErrorCallback = null;
        this.onOffsetOutOfRangeCallback = null;
        this.consumer = new kafka.Consumer(client, [], {
            autoCommit: false,
            fromOffset: true
        });

        this.consumer.on('message', (event) => {
            if(this.onMessageCallback) {
                this.onMessageCallback(event);
            }
        });

        this.consumer.on('error', (err) => {
            this.consumer.close(false, (err) => {
                this.onError(err);
            });
        });

        this.consumer.on('offsetOutOfRange', (err) => {
            if(this.onOffsetOutOfRangeCallback) {
                this.onOffsetOutOfRangeCallback(err);
            }
        });
    }
}

module.exports = SearchConsumer;