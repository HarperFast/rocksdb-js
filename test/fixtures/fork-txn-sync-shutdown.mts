import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import {
	getTransactionDrainWaits,
	isTransactionReadDelayedForTesting,
	isTransactionStagingDelayedForTesting,
	setTransactionReadDelayForTesting,
	setTransactionStagingDelayForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// A worker pauses inside a synchronous transaction call, past its read of the transaction's
// database handle, and this thread runs a foreign shutdown(). The shutdown must wait for the call.
const PAUSE_MS = 1000;

const operations = ['getSync', 'putSync', 'removeSync', 'getRange', 'getCount'] as const;
type Operation = (typeof operations)[number];
type Context = 'txn' | 'db';

if (isMainThread) {
	const [path, operation, context] = process.argv.slice(2) as [string, Operation, Context];
	assert(path);
	assert(operations.includes(operation));
	assert(context === 'txn' || context === 'db');
	const staging = operation === 'putSync' || operation === 'removeSync';
	const setPause = (delayMs: number) =>
		staging
			? setTransactionStagingDelayForTesting(delayMs ? 1 : 0, delayMs)
			: setTransactionReadDelayForTesting(delayMs);
	const isPaused = staging
		? isTransactionStagingDelayedForTesting
		: isTransactionReadDelayedForTesting;

	const db = RocksDatabase.open(path);
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-txn-sync-shutdown.mts'),
		{
			eval: true,
			workerData: { path, operation, context },
		}
	);
	try {
		assert.deepEqual(await once(worker, 'message'), ['ready']);
		const result = once(worker, 'message');
		setPause(PAUSE_MS);
		worker.postMessage('run');
		const deadline = Date.now() + 5000;
		while (!isPaused()) {
			assert(Date.now() < deadline, `${operation} did not reach its pause`);
			await delay(1);
		}
		const drainWaits = getTransactionDrainWaits();
		const started = Date.now();
		shutdown();
		const elapsed = Date.now() - started;
		assert.equal(registryStatus().length, 0);
		assert(!isPaused(), 'shutdown did not wait for the call');
		assert(elapsed >= PAUSE_MS / 2, `shutdown returned after ${elapsed}ms, during the call`);
		// A db-context call is drained before the transaction sweep; a txn-context one is the busy
		// transaction that sweep waits on, which must wake it once rather than be polled.
		const waits = getTransactionDrainWaits() - drainWaits;
		if (context === 'txn') {
			assert(waits >= 1 && waits <= 3, `transaction drain blocked ${waits} times`);
		} else {
			assert.equal(waits, 0);
		}
		// Range setup and the count scan recheck closing after the pause.
		const expected =
			operation === 'getRange'
				? ['error', 'Database not open']
				: operation === 'getCount'
					? ['error', 'Get count failed: Database is closing']
					: ['ok'];
		assert.deepEqual(await result, [expected]);
	} finally {
		setPause(0);
		await worker.terminate();
		db.close();
	}
	console.log('SUCCESS');
} else {
	const { path, operation, context } = workerData as {
		path: string;
		operation: Operation;
		context: Context;
	};
	const db = RocksDatabase.open(path);
	db.putSync('key', 'value');
	const txn = new Transaction(db.store);
	parentPort!.postMessage('ready');
	await once(parentPort!, 'message');
	try {
		const target = context === 'txn' ? txn : db;
		const options = context === 'txn' ? undefined : { transaction: txn };
		let result: unknown[] = ['ok'];
		if (operation === 'getSync') {
			assert.equal(target.getSync('key', options), 'value');
		} else if (operation === 'putSync') {
			target.putSync('key', 'updated', options);
		} else if (operation === 'removeSync') {
			target.removeSync('key', options);
		} else if (operation === 'getRange') {
			target.getKeys(options);
		} else {
			result = ['count', target.getKeysCount(options)];
		}
		parentPort!.postMessage(result);
	} catch (error) {
		assert(error instanceof Error);
		parentPort!.postMessage(['error', error.message]);
	}
	parentPort!.close();
}
