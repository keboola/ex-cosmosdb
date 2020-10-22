'use strict';

const { CosmosClient } = require("@azure/cosmos");
const UserError = require("./UserError.js");
const ApplicationError = require("./ApplicationError.js");

class Extractor {
    constructor() {
        // Check environment variables
        ['ENDPOINT', 'KEY', 'DATABASE_ID'].forEach(function(key) {
            if (process.env[key] === undefined) {
                throw new ApplicationError(`Missing "${key}" environment variable.`);
            }
        })

        this.endpoint = process.env['ENDPOINT'];
        this.key = process.env['KEY'];
        this.databaseId = process.env['DATABASE_ID'];
    }

    async testConnection() {
        await this.connect()
    }

    async extract() {

    }

    async connect() {
        try {
            const database = this.getDatabase();
            const info = await database.read()
            console.log(`Connected to the database: "${info.resource.id}"`);
            return database
        } catch (e) {
            switch (true) {
                case e.code === 'ERR_INVALID_URL':
                    throw new UserError(`Cannot connect: Invalid endpoint url "${this.endpoint}".`);

                case e.code === 404:
                    throw new UserError(`Cannot connect: Database not found.`);

                case e.message && e.message.includes('authorization token can\'t serve the request.'):
                    throw new UserError(`Cannot connect: Invalid key.`);
            }

            throw new UserError(`Cannot connect: ${e.message}`);
        }
    }

    getDatabase() {
        console.log(`Connecting to endpoint: "${this.endpoint}"`);
        const client = new CosmosClient({ endpoint: this.endpoint, key: this.key });
        return client.database(this.databaseId)
    }
}

module.exports = Extractor
