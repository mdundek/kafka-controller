const { Kafka } = require('kafkajs')

class Admin {
    
    /**
     * constructor
     */
    constructor(success, error) {
        this.kafka = new Kafka({
            clientId: 'admin-app',
            brokers: [`${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}`]
        });
        this.admin = this.kafka.admin();
        this.admin.connect().then(() => {
            success().then(() => {});
        }).catch(err => {
            error(err);
        });
    }

    /**
     * listTopics
     */
    async listTopics() {
        return await this.admin.listTopics();
    }

    /**
     * getTopicDetails
     */
    async getTopicDetails(topic) {
        return await this.admin.fetchTopicMetadata({ topics: [topic] });
    }

    /**
     * createTopic
     */
    async createTopic(topic, partitions, replicationFactor) {
        await this.admin.createTopics({
            topics: [{
                topic: topic,
                numPartitions: partitions,
                replicationFactor: replicationFactor ? replicationFactor : 1
            }],
        })
    }

    /**
     * deleteTopic
     * @param {*} topic 
     */
    async deleteTopic(topic) {
        await this.admin.deleteTopics({
            topics: [topic],
        })
    }

    /**
     * setTopicPartitions
     * @param {*} topic 
     * @param {*} partitions 
     */
    async createPartitions(topic, partitions) {
        await this.admin.createPartitions({
            topicPartitions: [{
                topic: topic,
                count: partitions
            }],
        })
    }
}
module.exports = Admin;