import { Transaction } from '../src/index.ts';
import { dbRunner, terminateWorker } from './lib/util.ts';
import { createWorkerBootstrapScript } from './lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

type Outcome = { code?: string; message?: string };
type Results = { foreign: Record<string, Outcome>; local: Record<string, Outcome> };

const ENTRY_POINTS = [
	'get',
	'getSync',
	'getMany',
	'getManySync',
	'getCount',
	'putSync',
	'removeSync',
	'getRange',
	'addEntry',
];

describe('Transaction environment ownership', () => {
	it('rejects by-id access from a worker that did not create the transaction', () =>
		dbRunner(async ({ db, dbPath }) => {
			db.putSync('k', 'committed');
			const txn = new Transaction(db.store);
			txn.putSync('k', 'staged');

			const worker = new Worker(
				createWorkerBootstrapScript('./test/workers/foreign-transaction-worker.mts'),
				{ eval: true, workerData: { path: dbPath } }
			);
			try {
				const results = await new Promise<Results>((resolve, reject) => {
					worker.on('error', reject);
					worker.on('message', (event) => {
						if (event.started) {
							worker.postMessage({ txnId: txn.id });
						} else {
							resolve(event);
						}
					});
				});

				for (const entryPoint of ENTRY_POINTS) {
					expect(results.foreign[entryPoint], entryPoint).toEqual({
						code: 'ERR_TRANSACTION_WRONG_ENV',
						message: `Transaction ${txn.id} belongs to another thread; a transaction can only be used from the thread that created it`,
					});
					expect(results.local[entryPoint], entryPoint).toEqual({});
				}

				expect(txn.getSync('k')).toBe('staged');
				expect(txn.getSync('foreign')).toBeUndefined();
				txn.commitSync();
				expect(db.getSync('k')).toBe('staged');
				expect(db.getSync('foreign')).toBeUndefined();
			} finally {
				worker.postMessage({ close: true });
				await terminateWorker(worker);
			}
		}));
});
