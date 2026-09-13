import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Commits one transaction on the family the main thread is about to drop. The
// parent's ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS seam parks the commit on its
// execution thread after admission and before `txn->Commit()`, so the drop
// lands while this commit holds its claim.
const db = RocksDatabase.open(workerData.path, {
	name: workerData.name,
	pessimistic: workerData.pessimistic,
});

parentPort?.on('message', async (message: { commit?: boolean; close?: boolean }) => {
	if (message.close) {
		db.close();
		parentPort?.postMessage({ closed: true });
		return;
	}
	if (!message.commit) return;

	const commit = db.transaction((txn) => {
		txn.putSync('committed-before-drop', Buffer.alloc(4096, 1));
	});
	// Give the microtask that dispatches the commit a turn before signalling.
	await new Promise((resolve) => setImmediate(resolve));
	parentPort?.postMessage({ committing: true });

	let error: string | undefined;
	try {
		await commit;
	} catch (e) {
		error = (e as Error).message;
	}
	parentPort?.postMessage({ done: true, error, lastError: db.getLastError() });
});

parentPort?.postMessage({ ready: true });
