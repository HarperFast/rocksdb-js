import { RocksDatabase, Transaction } from '../../src/index.ts';
import { NativeTransaction, setTransactionStagingDelayForTesting } from '../../src/load-binding.ts';
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
let stagingTransaction: Transaction | undefined;
let stagingDatabase: RocksDatabase | undefined;

parentPort?.on(
	'message',
	async (message: {
		commit?: boolean;
		close?: boolean;
		open?: boolean;
		barrier?: Int32Array;
		stage?: 'put' | 'delete' | 'timeout';
		finishStage?: boolean;
	}) => {
		if (message.close) {
			stagingDatabase?.close();
			db.close();
			closed = true;
			parentPort?.postMessage({ closed: true });
			return;
		}
		if (message.stage) {
			stagingDatabase = RocksDatabase.open(workerData.path, {
				name: 'meta',
				pessimistic: true,
				verificationTable: true,
			});
			stagingTransaction = new Transaction(stagingDatabase.store);
			stagingTransaction.putSync('key', 'new');
			if (message.stage === 'timeout') {
				parentPort?.postMessage({ stagingStarted: true });
			} else {
				setTransactionStagingDelayForTesting(1, 30_000);
			}
			let error: string | undefined;
			let errorCode: string | undefined;
			const started = Date.now();
			try {
				if (message.stage === 'put') {
					db.putSync('raced', 'value', { transaction: stagingTransaction });
				} else if (message.stage === 'timeout') {
					db.putSync('locked', 'candidate', { transaction: stagingTransaction });
				} else {
					db.removeSync('seed', { transaction: stagingTransaction });
				}
			} catch (e) {
				error = (e as Error).message;
				errorCode = (e as Error & { code?: string }).code;
			}
			parentPort?.postMessage({ staged: true, error, errorCode, elapsedMs: Date.now() - started });
			return;
		}
		if (message.finishStage && stagingTransaction && stagingDatabase) {
			let commitError: string | undefined;
			let commitErrorCode: string | undefined;
			const readThrough = stagingDatabase.getSync('key', { transaction: stagingTransaction });
			try {
				stagingTransaction.commitSync();
			} catch (e) {
				commitError = (e as Error).message;
				commitErrorCode = (e as Error & { code?: string }).code;
			}
			parentPort?.postMessage({
				stageFinished: true,
				commitError,
				commitErrorCode,
				readThrough,
			});
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
			workerData.scenario === 'handle-closed' || workerData.scenario === 'handle-closed-retry'
				? (() => {
						const txn = new NativeTransaction(db.store.db);
						txn.putSync(
							Buffer.from(db.store.encodeKey('committed-before-drop')),
							Buffer.alloc(4096, 1)
						);
						return new Promise<void>((resolve, reject) => txn.commit(() => resolve(), reject));
					})()
				: db.transaction((txn) => {
						txn.putSync('committed-before-drop', Buffer.alloc(4096, 1));
					});
		await new Promise((resolve) => setImmediate(resolve));
		parentPort?.postMessage({ committing: true });

		let error: string | undefined;
		let errorCode: string | undefined;
		try {
			await commit;
		} catch (e) {
			error = (e as Error).message;
			errorCode = (e as Error & { code?: string }).code;
		}
		parentPort?.postMessage({
			done: true,
			error,
			errorCode,
			lastError: closed ? null : db.getLastError(),
		});
	}
);

parentPort?.postMessage({ ready: true });
