const Base = require("./base");

class Producer extends Base {
    
    /**
     * constructor
     */
    constructor(client) {
        super(client);
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
module.exports = Producer;