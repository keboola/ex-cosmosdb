'use strict';

import promiseRetry from 'p-retry';
import { CosmosClient } from '@azure/cosmos';
import UserError from './UserError.js';
import ApplicationError from './ApplicationError.js';
import jsonStream from './jsonStream.js';

export default class Extractor {
  static async fetchNextWithRetry(iterator, maxTries) {
    return promiseRetry(async () => iterator.fetchNext(), {
      onFailedAttempt: (error) => {
        if (error.retriesLeft > 0) {
          console.log(`${error.message}. Retrying... [${error.attemptNumber}x]`);
        }
      },
      retries: maxTries - 1,
      factor: 2, // exponential factor
      minTimeout: 1000,
      maxTimeout: 30000,
    });
  }

  constructor() {
    // Check environment variables
    ['JSON_DELIMITER', 'ENDPOINT', 'KEY', 'DATABASE_ID'].forEach((key) => {
      if (!process.env[key]) {
        throw new ApplicationError(`Missing "${key}" environment variable.`);
      }
    });

    this.delimiter = JSON.parse(process.env.JSON_DELIMITER);
    this.endpoint = process.env.ENDPOINT;
    this.key = process.env.KEY;
    this.databaseId = process.env.DATABASE_ID;
  }

  async testConnection() {
    await this.getDatabase();
  }

  async extract() {
    // Check additional environment variables
    ['CONTAINER_ID', 'QUERY', 'MAX_TRIES'].forEach((key) => {
      if (!process.env[key]) {
        throw new ApplicationError(`Missing "${key}" environment variable.`);
      }
    });

    const containerId = process.env.CONTAINER_ID;
    const query = process.env.QUERY;
    const maxTries = process.env.MAX_TRIES;
    const container = await this.getContainer(containerId);

    try {
      await this.fetchAll(container, query, maxTries);
    } catch (e) {
      switch (true) {
        case e.code === 400:
          // Bad request, eg. bad SQL query
          throw new UserError(e.message);

        default:
          throw e;
      }
    }
  }

  async processPage(page, pageIndex, resolve) {
    if (!page.resources) {
      resolve(0);
      return;
    }

    let count = 0;
    page.resources.forEach((item) => {
      // Write item in JSON format, so PHP process can process it
      jsonStream.write(JSON.stringify(item));
      // Write delimiter
      jsonStream.write(this.delimiter);
      count += 1;
    });
    resolve(count);
  }

  async fetchAll(container, query, maxTries) {
    console.log(`Running query: "${query}"`);
    const iterator = container.items.query(query);

    let i = 0;
    let count = 0;
    let prevPage = null;

    while (true) {
      // Start fetching of the next page
      const page = iterator.hasMoreResults() ? await Extractor.fetchNextWithRetry(iterator, maxTries) : null;

      // Wait for the previous page to be processed,
      // ... so the outputs from the two pages are not mixed
      count += await prevPage;

      // End if no new page present
      if (!page) {
        break;
      }

      // Schedule the page processing,
      // ... so we can start fetching of the next page during processing of the current page
      const pageIndex = i;
      prevPage = new Promise((resolve) => process.nextTick(() => this.processPage(page, pageIndex, resolve)));
      i += 1;
    }

    // Wait until all data has been sent to the PHP process
    await new Promise((resolve) => jsonStream.end(resolve));
    console.log(`Fetched "${count}" items / "${i}" pages from the container "${container.id}".`);
  }

  async getContainer(containerId) {
    // Container is something like a table or a collection
    const database = await this.getDatabase();
    try {
      const container = database.container(containerId);
      const containerInfo = await container.read();
      console.log(`Connected to the container: "${containerInfo.resource.id}"`);
      return container;
    } catch (e) {
      switch (true) {
        case e.code === 404:
          throw new UserError(`Container "${containerId}" not found.`);

        default:
          throw e;
      }
    }
  }

  async getDatabase() {
    try {
      console.log(`Connecting to the endpoint: "${this.endpoint}"`);
      const client = new CosmosClient({ endpoint: this.endpoint, key: this.key });
      const database = client.database(this.databaseId);
      const databaseInfo = await database.read();
      console.log(`Connected to the database: "${databaseInfo.resource.id}"`);
      return database;
    } catch (e) {
      switch (true) {
        case e.code === 'ERR_INVALID_URL':
          throw new UserError(`Cannot connect: Invalid endpoint url "${this.endpoint}".`);

        case e.code === 404:
          throw new UserError('Cannot connect: Database not found.');

        case e.message && e.message.includes('authorization token can\'t serve the request.'):
          throw new UserError('Cannot connect: Invalid key.');

        default:
          const msg = e.message || e;
          throw new UserError(`Cannot connect: ${msg}`);
      }
    }
  }
}
