import { RocksDatabase } from '../../dist/index.mjs';
import type { Transaction } from '../../dist/index.mjs';
import { parentPort, threadId, workerData } from 'node:worker_threads';

/**
 * B6 contention worker: mixed keep / re-stamp async commits plus direct puts
 * against one shared stamped database from several worker_threads (the
 * process-global descriptor and watermark are shared across envs). Reports
 * every finalized stamp so the parent can assert global uniqueness.
 */
const db = RocksDatabase.open(workerData.path, { encoding: 'binary' });
const payload = Buffer.alloc(64, 'w');
const stamps: number[] = [];

for (let i = 0; i < workerData.commits; i++) {
	const restamp = i % 3 === 2;
	const txn = (await db.transaction((t: Transaction): Transaction => {
		if (restamp) {
			t.setTimestamp(5000.5 + (i % 7)); // duplicate-prone stale candidates
		}
		t.putSync(`w${threadId}-k${i & 255}`, payload);
		return t;
	})) as Transaction;
	stamps.push(txn.getCommittedLocalTime()!);
	if (i % 5 === 0) {
		// Direct puts exercise the unserialized claim path concurrently.
		db.putSync(`w${threadId}-direct${i & 255}`, payload);
		stamps.push((db.getSync(`w${threadId}-direct${i & 255}`) as Buffer).readDoubleBE(0));
	}
}

db.close();
parentPort?.postMessage({ stamps });
