import { registryStatus, RocksDatabase, shutdown } from '../../src/index.ts';
import {
	isTransactionCommitAdmissionDelayedForTesting,
	NativeTransaction,
	setTransactionCommitAdmissionDelayForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

if (isMainThread) {
	const [path, cache] = process.argv.slice(2);
	assert(path);
	const db = RocksDatabase.open(path);
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-commit-admission-shutdown.mts'),
		{
			eval: true,
			workerData: { path, cache },
		}
	);
	try {
		assert.deepEqual(await once(worker, 'message'), ['ready']);
		const result = once(worker, 'message');
		setTransactionCommitAdmissionDelayForTesting(10000);
		worker.postMessage('commit');
		const deadline = Date.now() + 5000;
		while (!isTransactionCommitAdmissionDelayedForTesting()) {
			assert(Date.now() < deadline, 'commit did not reach admission pause');
			await delay(1);
		}
		shutdown();
		assert(isTransactionCommitAdmissionDelayedForTesting(), 'shutdown outlasted the pause');
		assert.equal(registryStatus().length, 0);
		setTransactionCommitAdmissionDelayForTesting(0);
		assert.deepEqual(await result, [
			'Operation aborted: Database closed during transaction commit operation',
		]);
	} finally {
		setTransactionCommitAdmissionDelayForTesting(0);
		await worker.terminate();
		db.close();
	}
	console.log('SUCCESS');
} else {
	const db = RocksDatabase.open(workerData.path);
	if (workerData.cache === 'warm') {
		await db.transaction((txn) => txn.putSync('warm', true));
	}
	// Call the native commit directly so the TS aftercommit notification on the
	// closed database cannot replace its rejection with "Database not open".
	const txn = new NativeTransaction(db.store.db);
	parentPort!.postMessage('ready');
	await once(parentPort!, 'message');
	try {
		await new Promise<number | void>((resolve, reject) => txn.commit(resolve, reject));
		throw new Error('commit unexpectedly succeeded after shutdown');
	} catch (error) {
		assert(error instanceof Error);
		parentPort!.postMessage(error.message);
	}
	parentPort!.close();
}
