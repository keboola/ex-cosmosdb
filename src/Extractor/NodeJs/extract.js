const jsonStream = require('./jsonStream.js');

jsonStream.write("{\"abc\": \"xyz\"}\n");
jsonStream.write("\n---\n");
jsonStream.write("{\"123\": \"345\"}\n");
