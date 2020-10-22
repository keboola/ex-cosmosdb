const jsonStream = require('../../../src/Extractor/NodeJs/lib/jsonStream.js');

async function sleep() {
    await new Promise(resolve => setTimeout(resolve, 250));
}

(async () => {
    jsonStream.write('{"a": "1", "c": "d"}');
    await sleep();
    jsonStream.write("\n---\n");
    jsonStream.write('{"a": "2", "c": "d"}');
    jsonStream.write("\n---\n");
    jsonStream.write('{"a": "3", "c": "d"}');
    await sleep();
    jsonStream.write("\n---\n");
    jsonStream.write('{"a": "4", "c": "d"}');
})();
