# Cosmos DB Extractor

[![Build Status](https://travis-ci.com/keboola/ex-cosmosdb.svg?branch=master)](https://travis-ci.com/keboola/my-component)

[CosmosDB](https://azure.microsoft.com/en-us/free/cosmos-db) extractor for the [Keboola Connection](https://www.keboola.com).

## Configuration

The configuration `config.json` contains following properties in `parameters` key: 
- `db` - object (required): Configuration of the connection.
    - `endpoint` - string (required): Cosmos DB [SQL API](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-getting-started) endpoint.
    - `#key` - string (required): Access key.
    - `databaseId` - string (required): Database ID.
- `id` - integer (required): `id` of the config row.
- `name` - string (required): Name of the config row.
- `containerId` - string (required): Container is similar to table in the relational db, or collection in the MongoDB.
- `output` - string (required): Name of the output CSV file.
- `retries`- integer (optional): Number of the max retries if an error occurred.
- `ignoredKeys`- array (optional): 
    - CosmosDB automatically adds some metadata keys when the item is inserted.
    - By default, these keys are ignored: `["_rid", "_self", "_etag", "_attachments", "_ts"]`
- `incremental` - boolean (optional): Enables [Incremental Loading](https://help.keboola.com/storage/tables/#incremental-loading). Default `false`.
- `incrementalFetchingKey` - string (optional): Name of key for [Incremental Fetching](https://help.keboola.com/components/extractors/database/#incremental-fetching)
- `mode` - enum (optional)
    - `mapping` (default) - Documents are exported using specified `mapping`, [read more](https://github.com/keboola/php-csvmap).
    - `raw` - Documents are exported as plain JSON strings. CSV file will contain `id` and `data` columns.
- `mapping` - string - required for `mode` = `mapping`, [read more](https://github.com/keboola/php-csvmap).



- By default, Extractor exports all documents, using **the generated SQL query**.
    - Default query is `SELECT * FROM c`     
    - Query can be modified with these parameters:
    - `select` - string (optional), eg. `c.name, c.date`, default `*`, [read more](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-select).
       - For `raw` mode must be `id` field present in the query results.
    - `from` - string (optional), eg. `Families f`, default `c`, [read more](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-from).
    - `sort` - string (optional), eg. `c.date`, [read more](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-order-by).
    - `limit` - integer (optional), eg. `500`, [read more](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-offset-limit).

    
    
- Or you can set **a custom query** using parameter:
    - `query` - string (optional), eg. `SELECT f.name FROM Families f`


## Actions

Read more about actions [in KBC documentation](https://developers.keboola.com/extend/common-interface/actions/).

### Test Connection

Action `testConnection` tests the connection to the server.

The `parameters.db` node must be specified in the configuration.

## Data flow

- The connection to CosmosDB is established from the NodeJs code, using the official package [@azure/cosmos](https://www.npmjs.com/package/@azure/cosmos).
- There is no reliable driver for PHP now.
- The NodeJs code prints exported JSON documents to `JSON_STREAM_FD` file descriptor, from there they are read by the `JsonDecoder` PHP class.
- This communication is asynchronous.
- The code in PHP decodes the loaded JSON documents and writes them to the CSV files using [keboola/php-csvmap](https://github.com/keboola/php-csvmap).

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/ex-cosmosdb
cd ex-cosmosdb
docker-compose build
docker-compose run --rm dev composer install --no-scripts
docker-compose run --rm dev npm install
```

Create `.env` file with following variables:
```env
ENDPOINT=
KEY=
DATABASE_ID=
```


Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 
