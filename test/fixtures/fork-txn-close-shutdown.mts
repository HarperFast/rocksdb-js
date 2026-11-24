import { RocksDatabase, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A transaction close parks at the close latch on this thread while a worker runs shutdown(). A
// use-after-free need not crash, so the worker reports whether shutdown waited for the close.
const [path, mode] = process.argv.slice(2);
if (mode !== 'orphan' && mode !== 'abort' && mode !== 'commit-sync') {
	throw new Error(`unknown mode: ${mode}`);
}
if (!(Number(process.env.ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH) > 0)) {
	throw new Error('ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH must be set');
}
if (mode === 'orphan' && typeof globalThis.gc !== 'function') {
	throw new Error('the orphan mode needs --expose-gc');
}

const db = RocksDatabase.open(path);
const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/shutdown-during-commit-close-worker.mts'),
	{ eval: true, workerData: { path } }
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
if (!ready.ready) throw new Error(`worker failed to initialize: ${JSON.stringify(ready)}`);

if (mode === 'abort') {
	const txn = new Transaction(db.store);
	txn.putSync('k', 'aborted');
	txn.abort();
} else if (mode === 'commit-sync') {
	const txn = new Transaction(db.store);
	txn.putSync('k', 'committed');
	try {
		txn.commitSync();
	} catch (error) {
		// The after-commit notification finds the database shut down; the write is checked below.
		if (!/Database (not open|is closing)/.test(String(error))) throw error;
	}
} else {
	(() => {
		const txn = new Transaction(db.store);
		txn.putSync('k', 'orphaned');
	})();
	// The finalizer's close blocks this thread until the worker's shutdown has run.
	for (let i = 0; i < 200 && messages.length === 0; i++) {
		globalThis.gc!();
		await new Promise((resolve) => setImmediate(resolve));
	}
}

const result = await nextMessage();
if (!result.closed) throw new Error(`shutdown failed: ${JSON.stringify(result)}`);
if (!result.closeAwaited) throw new Error(`shutdown() did not wait for the ${mode} close`);
await worker.terminate();

const reopened = RocksDatabase.open(path);
try {
	const expected = mode === 'commit-sync' ? 'committed' : undefined;
	if (reopened.getSync('k') !== expected)
		throw new Error(`expected ${expected} after the ${mode} close`);
} finally {
	reopened.close();
}
console.log(JSON.stringify({ ok: true }));
