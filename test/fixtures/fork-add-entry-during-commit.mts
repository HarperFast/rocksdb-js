// Run from the repository root with ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS set: each
// commit is held after its log work, so the transaction stays registered and
// Committing while the owning env and a worker reach it by id.
import { RocksDatabase, Transaction } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

const notPending = /Transaction is not in pending state/;

async function waitForHeldCommit() {
	const deadline = performance.now() + 5000;
	while (!isTransactionCommitExecuteDelayedForTesting()) {
		assert(performance.now() < deadline, 'commit did not reach the execute delay');
		await delay(1);
	}
}

function entries(db: RocksDatabase): string[] {
	return Array.from(db.useLog('foo').query({ start: 0 }), ({ data }) => data.toString());
}

if (!isMainThread) {
	const db = RocksDatabase.open(workerData.dbPath);
	const log = db.useLog('foo');
	parentPort!.on('message', (txnId: number | null) => {
		if (txnId === null) {
			db.close();
			parentPort!.close();
			return;
		}
		let error: string | undefined;
		try {
			log.addEntry(Buffer.from('foreign'), txnId);
		} catch (err) {
			error = (err as Error).message;
		}
		parentPort!.postMessage({ error });
	});
	parentPort!.postMessage({ ready: true });
} else {
	const dbPath = process.argv[2];
	let db = RocksDatabase.open(dbPath);
	// Booted before the commit: worker startup can outlast the execute delay.
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-add-entry-during-commit.mts'),
		{ eval: true, workerData: { dbPath } }
	);
	const workerExit = once(worker, 'exit');
	try {
		const [ready] = await once(worker, 'message');
		assert.deepEqual(ready, { ready: true });

		const log = db.useLog('foo');
		const staged = new Transaction(db.store);
		const stagedLog = staged.useLog('foo');
		log.addEntry(Buffer.from('staged'), staged.id);
		await staged.put('staged', 'value');
		const stagedCommit = staged.commit();
		await waitForHeldCommit();
		assert.throws(() => log.addEntry(Buffer.from('late'), staged.id), notPending);
		assert.throws(() => stagedLog.addEntry(Buffer.from('late')), notPending);
		worker.postMessage(staged.id);
		const [{ error: workerError }] = await once(worker, 'message');
		assert(isTransactionCommitExecuteDelayedForTesting(), 'commit finished before the worker ran');
		// Rejected as not pending or, once by-id access is owner-only (#886), as foreign.
		assert(workerError !== undefined, 'a worker staged into a committing transaction');
		assert.doesNotMatch(workerError, /not found/);
		await stagedCommit;

		// Bound to the log but nothing staged: a late entry would start a batch nothing writes.
		const empty = new Transaction(db.store);
		const emptyLog = empty.useLog('foo');
		await empty.put('empty', 'value');
		const emptyCommit = empty.commit();
		await waitForHeldCommit();
		assert.throws(() => log.addEntry(Buffer.from('late'), empty.id), notPending);
		assert.throws(() => emptyLog.addEntry(Buffer.from('late')), notPending);
		await emptyCommit;

		await db.transaction((txn) => {
			log.addEntry(Buffer.from('after'), txn.id);
		});
		assert.deepEqual(entries(db), ['staged', 'after']);
	} finally {
		worker.postMessage(null);
		const [exitCode] = await workerExit;
		assert.equal(exitCode, 0);
		db.close();
	}

	db = RocksDatabase.open(dbPath);
	try {
		assert.equal(db.getSync('staged'), 'value');
		assert.equal(db.getSync('empty'), 'value');
		assert.deepEqual(entries(db), ['staged', 'after']);
	} finally {
		db.close();
	}
}
