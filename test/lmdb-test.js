const { open } = require('lmdb')
let db = open('./data.mdb');
let buffer = require('crypto').randomBytes(250000);
let key_num = new Uint32Array(1);
let key_buffer = Buffer.from(key_num.buffer);
console.log('process.pid', process.pid);
(async () => {
	let start = performance.now();
	let result;
	let promises = [];
	for (let i = 0; i < 10000; i++) {
		let id = Math.floor(Math.random() * 250000);
		key_num[0] = id;
		promises.push(db.put(key_buffer, buffer.subarray(0, Math.random() * 25)));
		if (promises.length > 500)
			await promises.shift();
		if (i % 1000 == 0) {
			console.log(i, (performance.now() - start) / i);
		}

	}
	await Promise.all(promises);
	console.log(performance.now() - start, result);
	start = performance.now();
	for (let i = 0; i < 100000; i++) {
		let id = Math.floor(Math.random() * 250000);
		key_num[0] = id;
		result = db.getBinaryFast(key_buffer);
		if (i % 1000 == 0) {
			console.log(i, (performance.now() - start) / i);
		}
	}
	console.log(performance.now() - start, result);

})();