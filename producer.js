const Base = require("./base");

class Producer extends Base {
    
    /**
     * constructor
     */
    constructor() {
        super();
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