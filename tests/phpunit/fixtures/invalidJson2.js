async function sleep() {
    await new Promise(resolve => setTimeout(resolve, 250));
}

(async () => {
    console.log('{"a": "1", "c": "d"}');
    await sleep();
    console.log('---');
    console.log('{"a": "2", "c": "d"}');
    console.log('---');
    console.log('{"a": "3", "c"....'); // <<<<<<<<<<<<<<<<
    await sleep();
    console.log('---');
    console.log('{"a": "4", "c": "d"}');
})();
