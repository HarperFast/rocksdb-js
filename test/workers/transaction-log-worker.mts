import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

parentPort?.on('message', async (event) => {
	if (event.addManyEntries) {
		for (let i = 0; i < event.count; i++) {
			const log = db.useLog('foo');
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from('world'), txn.id);
			});
		}
		parentPort?.postMessage({ done: true });
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
