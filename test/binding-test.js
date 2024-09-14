const binding = require('../binding')
let db = binding.db_init('/tmp/data');

binding.db_open(db, {
	keyEncoding: 'buffer',
	valueEncoding: 'buffer',
	parallelism: 4,
	pipelinedWrite: false,
	createIfMissing: true,
	// unorderedWrite: true,
	compaction: 'level',
	columns: {
		default: {
			cacheSize: 128e6,
			memtableMemoryBudget: 128e6,
			compaction: 'level'
			// optimize: 'point-lookup',
		}
	}
});
	let buffer = require('crypto').randomBytes(250000);
	let key_num = new Uint32Array(1);
	let key_buffer = Buffer.from(key_num.buffer);
	let start = performance.now();
	console.log('process.pid', process.pid)
	let result;
	for (let i = 0; i < 1000000; i++) {
		let id = Math.floor(Math.random() * 25000);
		key_num[0] = id;

//		let batch = binding.batch_init();
	//	binding.batch_put(batch, key_buffer, buffer.subarray(0, Math.random() * 250000));
	//	result = binding.batch_write(db, batch, {sync: false});
		result = binding.db_put(db, key_buffer, buffer.subarray(0, 100 + Math.random() * 1000));
		if (i % 1000 == 0) {
			console.log(i, (performance.now() - start) / i);
		}
	}
	start = performance.now();
	let uncached = 0;
	for (let i = 0; i < 400000; i++) {
		let id = Math.floor(Math.random() * 1000);
		key_num[0] = id;

		/*		let batch = binding.batch_init();
				binding.batch_put(batch, key_buffer, buffer.subarray(0, Math.random() * 250000));
				result = binding.batch_write(db, batch, {sync: false});*/
		result = binding.db_get(db, key_buffer);
		if (result === 2) uncached++;
		if (i % 1000 == 0) {
			console.log(i, (performance.now() - start) / i);
		}
	}
	console.log(performance.now() - start, result, uncached);
	binding.db_get_many(db, [Buffer.from('key')], { keyEncoding: 'buffer', valueEncoding: 'buffer', fillCache: true }, (err, sizes, data) => {
		if (err) {
			console.error(err);
		} else {
			console.log(data);
		}
	});
