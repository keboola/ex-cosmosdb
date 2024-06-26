'use strict';

import fs from 'fs';
import readline from 'readline';
import {CosmosClient} from '@azure/cosmos';


// List of the datasets
const datasets = [
    // If bulkSize > 1, then order of the documents is random
    {container: "simple", file: "simple.json", count: 5, bulkSize: 1},
    {container: "movie", file: "movie.json", count: 3, bulkSize: 1},
    {container: "fk_keys_check", file: "fk_keys_check.json", count: 4, bulkSize: 1},
    {container: "restaurant", file: "restaurant.json", count: 25, bulkSize: 25},
    {container: "empty", file: "empty.json", count: 0, bulkSize: 1}
];

class Importer {
    constructor() {
        // Check environment variables
        ['ENDPOINT', 'KEY', 'DATABASE_ID'].forEach(function (key) {
            if (process.env[key] === undefined) {
                throw new Error(`bootstrap: Missing "${key}" environment variable.`);
            }
        })

        this.endpoint = process.env['ENDPOINT'];
        this.key = process.env['KEY'];
        this.databaseId = process.env['DATABASE_ID'];
        this.client = new CosmosClient({endpoint: this.endpoint, key: this.key});
        this.database = this.client.database(this.databaseId);
    }

    async importAllDatasets() {
        for (let dataset of datasets) {
            await this.importDataset(dataset)
        }
    }

    async importDataset(dataset) {
        process.stdout.write(`bootstrap: Importing "${dataset.file}" to container "${dataset.container}": `)

        // Get container, it is similar to a table or an collection
        await this.database.containers.createIfNotExists({id: dataset.container});
        const container = this.database.container(dataset.container)

        // Check if items count match
        if (await this.checkItemsCount(container, dataset)) {
            // Yes, up to date, nothing to do
            process.stdout.write(` FOUND\n`)
            return;
        }

        // Import items
        for await (let bulk of this.getBulks(dataset)) {
            process.stdout.write('.');
            // Create `bulk_size` parallel requests and wait for all
            // We are using upsert, it is more safe for parallel builds
            const requests = bulk.map((item) => container.items.upsert(item));
            await Promise.all(requests);
        }
        process.stdout.write(" IMPORTED\n");
    }

    async checkItemsCount(container, dataset) {
        const count = await this.count(container);
        if (count === dataset.count) {
            // Number of items match, nothing to do
            return true
        } else if (count > 0) {
            // Number of items does not match -> clear container
            await container.delete();
            await this.database.containers.create({id: dataset.container});
            process.stdout.write(`cleared, `)
            return false
        }
    }

    async count(container) {
        return (await container.items.query('SELECT VALUE COUNT(1) FROM c').fetchNext()).resources[0];
    }

    async* getBulks(dataset) {
        let bulk = [];
        for await (let item of this.getItems(dataset)) {
            bulk.push(item);
            if (bulk.length === dataset.bulkSize) {
                yield bulk;
                bulk = [];
            }
        }

        if (bulk.length > 0) {
            yield bulk
        }
    }

    // Load items from JSON dataset file, ... one line = one JSON document
    async* getItems(dataset) {
        const fileStream = fs.createReadStream(`${__dirname}/../datasets/${dataset.file}`);
        const reader = readline.createInterface({input: fileStream, crlfDelay: Infinity});
        for await (let line of reader) {
            // Trim and remove coma from the end
            line = line.trim().replace(/,\s*$/, "");

            // Skip array start, end, empty line
            if (line === '[' || line === ']' || line === '') {
                continue;
            }

            yield JSON.parse(line);
        }
    }
}

async function main() {
    console.log('bootstrap: Importing datasets ...');
    const importer = new Importer();
    await importer.importAllDatasets()
    console.log('bootstrap: OK');
    console.log('');
}

main().catch((error) => {
    console.error("\nbootstrap: An error occurred while importing the datasets\n");
    console.error(error);
    process.exit(1);
});

