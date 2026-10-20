import { RocksDatabase } from '../../src/index.ts';
import { Transaction } from '../../src/transaction.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Deliberately registers no listener on `parentPort` and starts no timer: while
// this worker waits, the only thing that can keep its event loop alive is the
// rocksdb-js wake it is waiting for (rocksdb-js#797).
const { path, mode, key } = workerData as {
	path: string;
	mode: 'withLock' | 'tryLock' | 'park';
	key: string;
};
const db = RocksDatabase.open(path, { verificationTable: mode === 'park' });
let woken = 'ok';

if (mode === 'withLock') {
	const done = db.withLock(key, () => {});
	parentPort!.postMessage({ waiting: true });
	await done;
} else if (mode === 'tryLock') {
	await new Promise<void>((resolve, reject) => {
		if (db.tryLock(key, resolve)) {
			reject(new Error(`the worker acquired ${key}, which the main thread holds`));
			return;
		}
		parentPort!.postMessage({ waiting: true });
	});
} else {
	// The main thread holds a write intent on `key`. The first attempt loses a
	// conflict, parks on that intent until the main thread releases it, and
	// then the retry commits.
	let attempt = 0;
	await db.transaction(
		async (txn: Transaction) => {
			attempt++;
			await txn.put(key, `attempt-${attempt}`);
			if (attempt === 1) {
				const conflicting = new Transaction(db.store, { coordinatedRetry: true });
				await conflicting.put(key, 'conflicting');
				await conflicting.commit();
				parentPort!.postMessage({ waiting: true });
			}
		},
		{ coordinatedRetry: true, maxRetries: 3 }
	);
	woken = String(await db.get(key));
}

parentPort!.postMessage({ woken });
db.close();
