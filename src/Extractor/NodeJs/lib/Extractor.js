const { CosmosClient } = require("@azure/cosmos");
const UserError = require("./UserError.js");

class Extractor {
    async testConnection() {
        //throw new UserError("some bad error");
    }

    async extract() {

    }
}

module.exports = Extractor
