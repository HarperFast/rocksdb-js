import { RocksDatabase, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A use-after-free here does not reliably crash, so whether shutdown() waited is what is asserted.
const path = process.argv[2];
if (!(Number(process.env.ROCKSDB_JS_TXN_CLOSE_DELAY_MS) > 0)) {
	throw new Error('ROCKSDB_JS_TXN_CLOSE_DELAY_MS must be set');
}
const fail = (message: string): never => {
	throw new Error(message);
};

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
if (!ready.ready) fail(`worker failed to initialize: ${JSON.stringify(ready)}`);

const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
try {
	await txn.commit();
} catch (error) {
	// The after-commit notification can find the database already shut down; the commit itself
	// is checked below by reopening.
	if (!/Database (not open|is closing)/.test(String(error))) throw error;
}

const result = await nextMessage();
if (!result.shutdown) fail(`shutdown failed: ${JSON.stringify(result)}`);
if (!result.closeDelayedAtShutdown)
	fail('shutdown() started after the close delay; nothing was tested');
if (result.closeStillDelayed) {
	fail("shutdown() returned while the commit completion's close() was still running");
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
