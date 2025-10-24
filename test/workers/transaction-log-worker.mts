import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

parentPort?.on('message', event => {
	if (event.addManyEntries) {
		for (let i = 0; i < event.count; i++) {
			const log = db.useLog('foo');
			log.addEntry(Date.now(), Buffer.from('hello'));
		}
		parentPort?.postMessage({ done: true });
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
