'use strict';

const Extractor = require('./lib/Extractor.js');
const UserError = require("./lib/UserError.js");
const ApplicationError = require("./lib/ApplicationError.js");

async function main() {
    const extractor = new Extractor();
    await extractor.extract()
}

main().catch((error) => {
    // User error
    if (error instanceof UserError) {
        console.error(error.message)
        process.exit(1);
    }

    // Application error
    console.error(error instanceof ApplicationError ? error.message : error);
    process.exit(2);
});

