import { RocksDatabase } from '../../dist/index.mjs';
import { randomBytes } from 'node:crypto';
import { parentPort, workerData } from 'node:worker_threads';

parentPort?.on('message', (event) => {
	if (event.runTransactions10k) {
		runTransactions10k();
	} else if (event.runTransactions10kWithLogs) {
		runTransactions10kWithLogs();
	}
});

async function runTransactions10k() {
	const db = RocksDatabase.open(workerData.path);
	// Commits can finish out of order, so the last one settling does not mean the others have.
	let pending: Promise<unknown>[] = [];

	for (let i = 0; i < workerData.iterations; i++) {
		pending.push(
			db.transaction((transaction) => {
				db.putSync(randomBytes(16).toString('hex'), 'hello world', { transaction });
			})
		);
		if (i % 20 === 0) {
			await Promise.all(pending);
			pending = [];
		}
	}

	await Promise.all(pending);

	db.close();
	parentPort?.postMessage({ done: true });
	parentPort?.close();
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

	db.close();
	parentPort?.postMessage({ done: true });
	parentPort?.close();
}
