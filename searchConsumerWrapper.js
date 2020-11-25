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
        try {
            this.consumer = new kafka.Consumer(client, [], {
                autoCommit: false,
                fromOffset: true
            });
        
            this.consumer.on('message', this._onMessage.bind(this));
            this.consumer.on('error', this._onError.bind(this));
            this.consumer.on('offsetOutOfRange', this._onOffsetOutOfRange.bind(this));
        } catch (error) {
            this.cleanup();
            this.onError(error);
        }
    }

    _onMessage(event) {
        if(this.onMessageCallback) {
            this.onMessageCallback(event);
        }
    }

    _onError(err) {
        if(this.consumer) {
            this.consumer.close(false, (err) => {
                this.cleanup();
                this.onError(err);
            });
        } else {
            this.onError(err);
        }
    }

    _onOffsetOutOfRange(err) {
        if(this.onOffsetOutOfRangeCallback) {
            this.onOffsetOutOfRangeCallback(err);
        }
    }

    cleanup() {
        try {
            this.consumer.removeListener('message', this._onMessage);
            this.consumer.removeListener('error', this._onError);
            this.consumer.removeListener('offsetOutOfRange', this._onOffsetOutOfRange);
        } catch (error) {}
    }
}

module.exports = SearchConsumer;