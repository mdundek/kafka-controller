const { Pool } = require('pg');

class DBController {

    /**
     * init
     */
    static init() {
        try {
            this.pool = new Pool({
                user: process.env.DB_USER,
                host: process.env.DB_HOST,
                database: process.env.DB_KAFKA_NAME,
                password: process.env.DB_PASS,
                port: process.env.DB_PORT,
            });
            this.pool.on('error', (err, client) => {
                console.error('Unexpected error on idle client', err);
            });
        } catch (error) {
                console.log("PG ERROR", error);
        }
    }

    /**
     * getAllClusterMasters
     */
    static async getTopicOffset(groupId, topic, partition) {
        let client = null;
        try {
            client = await this.pool.connect();
            const res = await client.query(`SELECT topic_offset."offset" FROM topic_offset WHERE "topic" = $1 AND "partition" = $2 AND "groupId" = $3`, [topic, partition, groupId]);
            return res.rows.length == 1 ? res.rows[0] : null;
        } 
        catch (error) {
            console.log("PG ERROR", error);
            client = null;
            throw error;
        } finally {
            if(client)
                client.release();
        }
    }

    /**
     * createTopicOffset
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} offset 
     */
    static async createTopicOffset(groupId, topic, partition, offset) {
        let client = null;
        try {
            client = await this.pool.connect();
            let query = `INSERT INTO topic_offset ("groupId", "topic", "partition", "offset") VALUES ($1, $2, $3, $4)`;
            let values = [groupId, topic, partition, offset];
            return await client.query(query, values);
        } catch (error) {
            console.log("PG ERROR", error);
            client = null;
            throw error;
        } finally {
            if(client)
                client.release();
        }
    }

    /**
     * updateTopicOffset
     * @param {*} topic 
     * @param {*} partition 
     * @param {*} offset 
     */
    static async updateTopicOffset(groupId, topic, partition, offset) {
        let client = null;
        try {
            client = await this.pool.connect();
            let query = `UPDATE topic_offset SET "offset" = $1 WHERE "topic" = $2 AND "partition" = $3 AND "groupId" = $4`;
            let values = [offset, topic, partition, groupId];
            return await client.query(query, values);
        } catch (error) {
            console.log("PG ERROR", error);
            client = null;
            throw error;
        } finally {
            if(client)
                client.release();
        }
    }
}

module.exports = DBController;