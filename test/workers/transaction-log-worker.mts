import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

parentPort?.on('message', async (event) => {
	if (event.addManyEntries) {
		// The main thread may call purgeLogs({ destroy: true }) which can close
		// a transaction log store while we're trying to use it. This is expected
		// behavior in concurrent scenarios - we just catch the error and continue.
		for (let i = 0; i < event.count; i++) {
			try {
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.from('world'), txn.id);
				});
			} catch {
				// Errors like "Transaction log store is closed" are expected when
				// the main thread is destroying stores. Just continue to next iteration.
			}
		}
		parentPort?.postMessage({ done: true });
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
