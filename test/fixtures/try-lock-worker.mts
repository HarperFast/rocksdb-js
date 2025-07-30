import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

function getLock() {
	if (db.tryLock('foo', getLock)) {
		parentPort?.postMessage({ locked: true });
	}
}

getLock();

parentPort?.on('message', event => {
	if (event.unlock) {
		db.unlock('foo');
		parentPort?.postMessage({ unlocked: true });
	}
	if (event.lock) {
		getLock();
	}
});

parentPort?.postMessage({ started: true, hasLock: db.hasLock('foo') });
