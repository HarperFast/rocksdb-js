import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import {
	isTransactionCommitAdmissionDelayedForTesting,
	setTransactionCommitAdmissionDelayForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

if (isMainThread) {
	const [path, cache, phase, family] = process.argv.slice(2);
	assert(path);
	assert(phase === 'before' || phase === 'after');
	assert(family === undefined || family === 'dropped');
	const afterRegistration = phase === 'after';
	const db = RocksDatabase.open(path);
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-commit-admission-shutdown.mts'),
		{
			eval: true,
			workerData: { path, cache, dropped: family === 'dropped' },
		}
	);
	try {
		assert.deepEqual(await once(worker, 'message'), ['ready']);
		const result = once(worker, 'message');
		setTransactionCommitAdmissionDelayForTesting(10000, afterRegistration);
		worker.postMessage('commit');
		const deadline = Date.now() + 5000;
		while (!isTransactionCommitAdmissionDelayedForTesting()) {
			assert(Date.now() < deadline, 'commit did not reach admission pause');
			await delay(1);
		}
		const shutdownStart = Date.now();
		shutdown();
		if (afterRegistration) {
			assert(Date.now() - shutdownStart < 5000, 'the pause outlived the close that cancelled it');
			assert(
				!isTransactionCommitAdmissionDelayedForTesting(),
				'shutdown did not wait for the commit'
			);
		} else {
			assert(isTransactionCommitAdmissionDelayedForTesting(), 'shutdown outlasted the pause');
		}
		assert.equal(registryStatus().length, 0);
		setTransactionCommitAdmissionDelayForTesting(0);
		assert.deepEqual(
			await result,
			afterRegistration
				? [
						[
							'ERR_ABORTED',
							'Operation aborted: Database closed during transaction commit operation',
						],
					]
				: [['ERR_TRANSACTION_CLOSING', 'Transaction is closing']]
		);
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
	const txn = new Transaction(db.store);
	// The closing database outranks a dropped column family the transaction wrote to.
	if (workerData.dropped) {
		const doomed = RocksDatabase.open(workerData.path, { name: 'doomed' });
		doomed.putSync('k', 'v', { transaction: txn });
		RocksDatabase.open(workerData.path, { name: 'doomed' }).dropSync();
	}
	parentPort!.postMessage('ready');
	await once(parentPort!, 'message');
	try {
		await txn.commit();
		throw new Error('commit unexpectedly succeeded after shutdown');
	} catch (error) {
		assert(error instanceof Error);
		parentPort!.postMessage([(error as { code?: string }).code, error.message]);
	}
	parentPort!.close();
}
