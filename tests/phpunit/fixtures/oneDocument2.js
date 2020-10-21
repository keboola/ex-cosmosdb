const jsonStream = require('./../../../src/Extractor/NodeJs/jsonStream.js');

jsonStream.write('{"a": "b", "c": "d"}');
// Json Decoder must be foolproof, try an delimiter on the end
jsonStream.write("\n---\n");
jsonStream.write(' ');
