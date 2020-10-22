const Extractor = require('./lib/Extractor.js');
const UserError = require("./lib/UserError.js");

const extractor = new Extractor();
extractor.testConnection().catch((error) => {
    // User error
    if (error instanceof UserError) {
        console.error(error.message)
        process.exit(1);
    }

    // Application error
    console.error(error);
    process.exit(2);
});

