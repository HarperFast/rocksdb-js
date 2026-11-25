import { shutdown, RocksDatabase, Transaction } from '../../src/index.ts';
import {
	isTransactionCommitAdmissionDelayedForTesting,
	setTransactionCommitAdmissionDelayForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// ROCKSDB_JS_CLOSE_FAILURE stops shutdown() before it closes the transaction, leaving only the
// descriptor closing. With `dropped`, the transaction also wrote to a dropped column family, which
// the closing database outranks.
if (isMainThread) {
	const [path, family] = process.argv.slice(2);
	assert(path);
	assert(family === undefined || family === 'dropped');
	const closeFails = process.env.ROCKSDB_JS_CLOSE_FAILURE === '1';
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-commit-sync-closing.mts'),
		{ eval: true, workerData: { path, dropped: family === 'dropped' } }
	);
	try {
		assert.deepEqual(await once(worker, 'message'), ['ready']);
		const result = once(worker, 'message');
		setTransactionCommitAdmissionDelayForTesting(10000);
		worker.postMessage('commit');
		const deadline = Date.now() + 5000;
		while (!isTransactionCommitAdmissionDelayedForTesting()) {
			assert(Date.now() < deadline, 'commitSync() did not reach the admission pause');
			await delay(1);
		}
		if (closeFails) {
			assert.throws(() => shutdown(), /Injected database close failure/);
		} else {
			shutdown();
		}
		assert(isTransactionCommitAdmissionDelayedForTesting(), 'shutdown() outlasted the pause');
		setTransactionCommitAdmissionDelayForTesting(0);
		assert.deepEqual(
			await result,
			closeFails
				? [
						{
							code: 'ERR_ABORTED',
							message: 'Operation aborted: Database closed during transaction commit operation',
						},
					]
				: [{ code: 'ERR_TRANSACTION_CLOSING', message: 'Transaction is closing' }]
		);
		if (closeFails) {
			shutdown();
		}
	} finally {
		setTransactionCommitAdmissionDelayForTesting(0);
		await worker.terminate();
	}
	console.log('SUCCESS');
} else {
	const db = RocksDatabase.open(workerData.path);
	const txn = new Transaction(db.store);
	if (workerData.dropped) {
		const doomed = RocksDatabase.open(workerData.path, { name: 'doomed' });
		doomed.putSync('k', 'v', { transaction: txn });
		RocksDatabase.open(workerData.path, { name: 'doomed' }).dropSync();
	}
	parentPort!.postMessage('ready');
	await once(parentPort!, 'message');
	try {
		txn.commitSync();
		parentPort!.postMessage({ committed: true });
	} catch (error) {
		parentPort!.postMessage({
			code: (error as { code?: string }).code,
			message: (error as Error).message,
		});
	}
	parentPort!.close();
}
