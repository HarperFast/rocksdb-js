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
	const [path, cache, phase] = process.argv.slice(2);
	assert(path);
	assert(phase === 'before' || phase === 'after');
	const afterRegistration = phase === 'after';
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
		// After registration the shutdown below blocks until the pause ends, so keep it short.
		setTransactionCommitAdmissionDelayForTesting(
			afterRegistration ? 500 : 10000,
			afterRegistration
		);
		worker.postMessage('commit');
		const deadline = Date.now() + 5000;
		while (!isTransactionCommitAdmissionDelayedForTesting()) {
			assert(Date.now() < deadline, 'commit did not reach admission pause');
			await delay(1);
		}
		shutdown();
		if (afterRegistration) {
			// The commit holds its transaction registration while it reads `dbHandle`, so the
			// sweep's close() of the transaction waits for it rather than resetting it underneath.
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
				: // Refused at registration, before reading the `dbHandle` the sweep reset.
					[['ERR_TRANSACTION_CLOSING', 'Transaction is closing']]
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
		parentPort!.postMessage([(error as NodeJS.ErrnoException).code, error.message]);
	}
	parentPort!.close();
}
