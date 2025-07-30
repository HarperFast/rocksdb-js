import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData, threadId } from 'node:worker_threads';
import { setTimeout as delay } from 'node:timers/promises';

const db = RocksDatabase.open(workerData.path);

async function getLock() {
	console.log(`worker ${threadId} locking`);
	await db.withLock('foo', async () => {
		console.log(`worker ${threadId} lock`);
		parentPort?.postMessage({ locked: true });
		await delay(100);
		console.log(`worker ${threadId} unlock`);
	});
	parentPort?.postMessage({ unlocked: true });
}

getLock();

parentPort?.on('message', event => {
	console.log(`worker ${threadId} message`, event);
	if (event.lock) {
		getLock();
	}
});

parentPort?.postMessage({ started: true, hasLock: db.hasLock('foo') });
