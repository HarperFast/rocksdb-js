import { RocksDatabase } from '../../src/index.js';
import { parentPort } from 'node:worker_threads';

RocksDatabase.on('global-events-test:from-main', (value) => {
	parentPort?.postMessage({ fromMain: value });
});

parentPort?.on('message', (event) => {
	if (event.notify) {
		RocksDatabase.notify('global-events-test:from-worker', event.notify);
	} else if (event.exit) {
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
