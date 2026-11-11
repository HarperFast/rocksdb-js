import { RocksDatabase, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// Run with ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS and ROCKSDB_JS_TXN_CLOSE_DELAY_MS: commitSync() parks
// before its RocksDB commit, while a worker runs shutdown(), and again inside its own close() of
// the committed transaction. shutdown() must wait out both rather than destroy the database under
// them; a use-after-free there does not reliably crash, so the timing is what is asserted.
const path = process.argv[2];
const executeDelayMs = Number(process.env.ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS);
const closeDelayMs = Number(process.env.ROCKSDB_JS_TXN_CLOSE_DELAY_MS);
if (!(executeDelayMs > 0 && closeDelayMs > 0)) throw new Error('both delay seams must be set');
const fail = (message: string): never => {
	throw new Error(message);
};

const db = RocksDatabase.open(path);
const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/shutdown-during-commit-sync-worker.mts'),
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
if (!ready.ready) fail(`worker failed to initialize: ${JSON.stringify(ready)}`);

const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
const commitStarted = Date.now();
try {
	txn.commitSync();
} catch (error) {
	// The after-commit notification can find the database already shut down; the commit itself
	// is checked below by reopening.
	if (!/Database (not open|is closing)/.test(String(error))) throw error;
}

const result = await nextMessage();
if (!result.shutdown) fail(`shutdown failed: ${JSON.stringify(result)}`);
if (result.delayStillActive) fail('shutdown() returned while commitSync() was still in its commit');
// Half the close delay as slack: without the wait, shutdown() returns about executeDelayMs in.
if (result.at - commitStarted < executeDelayMs + closeDelayMs / 2) {
	fail(`shutdown() returned ${result.at - commitStarted}ms in, inside commitSync()'s close()`);
}
await worker.terminate();

const reopened = RocksDatabase.open(path);
try {
	const value = reopened.getSync('k');
	if (value !== 'committed') fail(`expected the commit to survive shutdown, read ${value}`);
} finally {
	reopened.close();
}
console.log(JSON.stringify({ ok: true }));
