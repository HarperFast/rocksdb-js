import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';
import { randomBytes } from 'node:crypto';

// top-level await not supported in Node.js 18
(async () => {
	const db = RocksDatabase.open(workerData.path);
	let last: Promise<void | PromiseLike<void>> | undefined;

	for (let i = 0; i < workerData.iterations; i++) {
		last = db.transaction((transaction) => {
			db.putSync(randomBytes(16).toString('hex'), 'hello world', { transaction });
		});
		if (i % 20 === 0) {
			await last;
		}
	}

	if (last) {
		await last;
	}

	parentPort?.postMessage({ done: true });
})();
