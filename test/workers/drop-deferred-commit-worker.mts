import { RocksDatabase } from '../../src/index.ts';
import { NativeTransaction } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Commits one transaction on the family the main thread is about to drop. The
// parent's ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS seam parks the commit on its
// execution thread after admission and before `txn->Commit()`, so the drop
// lands while this commit holds its claim.
const db = RocksDatabase.open(workerData.path, {
	name: workerData.name,
	pessimistic: workerData.pessimistic,
});
let closed = false;

parentPort?.on(
	'message',
	async (message: { commit?: boolean; close?: boolean; open?: boolean; barrier?: Int32Array }) => {
		if (message.close) {
			db.close();
			closed = true;
			parentPort?.postMessage({ closed: true });
			return;
		}
		if (message.open) {
			// Open the name fresh while the main thread retries a failed physical
			// drop of its previous generation.
			let error: string | undefined;
			let seed: unknown;
			if (message.barrier) {
				Atomics.wait(message.barrier, 0, 0);
			}
			try {
				const fresh = RocksDatabase.open(workerData.path, {
					name: workerData.name,
					pessimistic: workerData.pessimistic,
				});
				seed = fresh.getSync('seed');
				fresh.close();
			} catch (e) {
				error = (e as Error).message;
			}
			parentPort?.postMessage({ opened: true, error, seed });
			return;
		}
		if (!message.commit) return;

		const commit =
			workerData.scenario === 'handle-closed'
				? (() => {
						const txn = new NativeTransaction(db.store.db);
						txn.putSync(Buffer.from('committed-before-drop'), Buffer.alloc(4096, 1));
						return new Promise<void>((resolve, reject) => txn.commit(() => resolve(), reject));
					})()
				: db.transaction((txn) => {
						txn.putSync('committed-before-drop', Buffer.alloc(4096, 1));
					});
		await new Promise((resolve) => setImmediate(resolve));
		parentPort?.postMessage({ committing: true });

		let error: string | undefined;
		try {
			await commit;
		} catch (e) {
			error = (e as Error).message;
		}
		parentPort?.postMessage({ done: true, error, lastError: closed ? null : db.getLastError() });
	}
);

parentPort?.postMessage({ ready: true });
