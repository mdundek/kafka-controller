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
            (async() => {
                let client = await this.pool.connect();
                try {
                    let result = await client.query("SELECT 1 FROM pg_database WHERE datname='kafka-offsets'");
                    if(result.rows.length == 0)
                        await client.query('CREATE DATABASE "kafka-offsets"');

                    const res = await client.query(`SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE  table_schema = 'public'
                        AND    table_name   = 'topic_offset'
                        );`, []);
                    if(!res.rows[0].exists) {
                        await client.query(`CREATE SEQUENCE topic_offset_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 17 CACHE 1`, []);
                        await client.query(`CREATE TABLE "public"."topic_offset" (
                            "id" integer DEFAULT nextval('topic_offset_id_seq') NOT NULL,
                            "topic" character varying(255) NOT NULL,
                            "partition" character varying(255) NOT NULL,
                            "offset" integer NOT NULL,
                            "groupId" character varying(255) NOT NULL
                        ) WITH (oids = false)`, []);
                    }
               
                } catch (_e) {
                    console.log("PG INIT DB ERROR", _e.message);
                }
                finally {
                    client.release();
                }
            })();
        } catch (error) {
            console.log("PG connect ERROR", error.message);
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
            console.log("PG ERROR getTopicOffset", error.message);
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
            console.log("PG ERROR createTopicOffset", error.message);
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
            console.log("PG ERROR updateTopicOffset", error.message);
            client = null;
            throw error;
        } finally {
            if(client)
                client.release();
        }
    }
}

module.exports = DBController;