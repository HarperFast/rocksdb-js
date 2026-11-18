import { RocksDatabase, Transaction } from '../../src/index.ts';
import { isTransactionCloseGuardedAtDrainForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A use-after-free need not crash; assert the operation count captured at the descriptor drain.
// argv: <path> <shutdown|destroy>. With ROCKSDB_JS_COMMIT_DISPATCH_FAILURE the completion is dropped
// as if its env were gone, so the commit thread's fallback closes the transaction and the commit
// promise never settles; with ROCKSDB_JS_COMMIT_THREAD_START_FAILURE as well, that fallback runs
// inline on this thread, inside commit().
const path = process.argv[2];
const closer = process.argv[3];
if (closer !== 'shutdown' && closer !== 'destroy') {
	throw new Error(`unknown closer: ${closer}`);
}
if (!(Number(process.env.ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH) > 0)) {
	throw new Error('ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH must be set');
}
const droppedCompletion = Number(process.env.ROCKSDB_JS_COMMIT_DISPATCH_FAILURE) > 0;
const inlineCommit = Number(process.env.ROCKSDB_JS_COMMIT_THREAD_START_FAILURE) > 0;
if (inlineCommit && !droppedCompletion) {
	throw new Error(
		'ROCKSDB_JS_COMMIT_THREAD_START_FAILURE needs ROCKSDB_JS_COMMIT_DISPATCH_FAILURE'
	);
}
const fail = (message: string): never => {
	throw new Error(message);
};

const db = RocksDatabase.open(path);
const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/shutdown-during-commit-close-worker.mts'),
	{ eval: true, workerData: { path, closer } }
);
const messages: any[] = [];
let notify: (() => void) | undefined;
worker.on('message', (message) => {
	messages.push(message);
	notify?.();
});
const nextMessage = async (): Promise<any> => {
	while (messages.length === 0) {
		await new Promise<void>((resolve) => (notify = resolve));
	}
	return messages.shift();
};

const ready = await nextMessage();
if (!ready.ready) fail(`worker failed to initialize: ${JSON.stringify(ready)}`);

const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
let settled = false;
if (droppedCompletion) {
	void txn.commit().finally(() => (settled = true));
	// Inline, commit() returns only after the worker's close has passed the drain.
	if (inlineCommit && !isTransactionCloseGuardedAtDrainForTesting()) {
		fail(
			`${closer}() reached the drain inside commit() without the fallback close's operation registered`
		);
	}
} else {
	try {
		await txn.commit();
	} catch (error) {
		// The after-commit notification can find the database already shut down; the commit itself
		// is checked below by reopening.
		if (!/Database (not open|is closing)/.test(String(error))) throw error;
	}
}

const result = await nextMessage();
if (!result.closed) fail(`${closer}() failed: ${JSON.stringify(result)}`);
if (!result.closeGuardedAtDrain) {
	fail(`${closer}() reached the drain without the settled commit's close operation registered`);
}
await worker.terminate();
if (settled) fail('the dropped completion settled the commit promise');

if (closer === 'shutdown') {
	const reopened = RocksDatabase.open(path);
	try {
		const value = reopened.getSync('k');
		if (value !== 'committed') fail(`expected the commit to survive shutdown, read ${value}`);
	} finally {
		reopened.close();
	}
}
console.log(JSON.stringify({ ok: true }));
