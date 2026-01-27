import { randomBytes } from 'node:crypto';
import { parentPort, workerData } from 'node:worker_threads';
import { RocksDatabase } from '../../src/index.js';

parentPort?.on('message', event => {
	if (event.runTransactions10k) {
		runTransactions10k();
	} else if (event.runTransactions10kWithLogs) {
		runTransactions10kWithLogs();
	}
});

async function runTransactions10k() {
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
}

async function runTransactions10kWithLogs() {
	const db = RocksDatabase.open(workerData.path);

	const log = db.useLog('foo');

	for (let i = 0; i < workerData.iterations; i++) {
		await db.transaction((transaction) => {
			db.putSync(randomBytes(16).toString('hex'), 'hello world', { transaction });
			const size = Math.floor(Math.random() * (5000 - 100 + 1)) + 100; // Random size between 100 bytes and 5KB
			const data = randomBytes(size);
			log.addEntry(data, transaction.id);
		});
	}

	parentPort?.postMessage({ done: true });
}
