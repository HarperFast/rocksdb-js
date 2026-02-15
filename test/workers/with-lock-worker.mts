import { RocksDatabase } from '../../src/index.js';
import { setTimeout as delay } from 'node:timers/promises';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

async function getLock() {
	await db.withLock('foo', async () => {
		parentPort?.postMessage({ locked: true });
		await delay(100);
	});
	parentPort?.postMessage({ unlocked: true });
}

getLock();

parentPort?.on('message', (event) => {
	if (event.lock) {
		getLock();
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true, hasLock: db.hasLock('foo') });
