import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

db.addListener('parent-event', (value) => {
	parentPort?.postMessage({ parentEvent: value });
});

parentPort?.on('message', (event) => {
	if (event.notify) {
		db.notify('worker-event', 'foo');
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
