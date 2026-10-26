/**
 * Transaction iterator pooling against a stalling WriteBufferManager: a
 * synchronous write can block the JS thread waiting for memory that idle
 * pooled iterators pin, and only that thread's trim would release them. So
 * pooling is off while stalls are enabled, and enabling them closes every idle
 * pooled iterator before the manager starts stalling. Prints pool counters and
 * closable counts as JSON.
 */
import { registryStatus, RocksDatabase } from '../../src/index.ts';
import { iteratorPoolStats } from '../../src/load-binding.ts';
import { Transaction } from '../../src/transaction.ts';
import { once } from 'node:events';
import { isMainThread, Worker, workerData } from 'node:worker_threads';

if (!isMainThread) {
	// Another env turns stalls on: it closes the main thread's idle iterators.
	RocksDatabase.config({ writeBufferManagerAllowStall: true });
	const signal = new Int32Array(workerData as SharedArrayBuffer);
	Atomics.store(signal, 0, 1);
	Atomics.notify(signal, 0);
	process.exit(0);
}

const dbPath = process.argv[2];
const fromWorker = process.argv[3] === 'worker';
if (!dbPath) {
	throw new Error('Missing database path');
}

RocksDatabase.config({
	writeBufferManagerSize: 64 * 1024 * 1024,
	writeBufferManagerAllowStall: false,
});
const db = RocksDatabase.open(dbPath);
db.putSync('a', 'a');
// The registry may report the path as given or resolved (macOS /var -> /private/var).
const closables = () =>
	registryStatus().find((entry) => entry.path === dbPath || entry.path === db.store.identityPath)!
		.closables;

function ranges(txn: Transaction): { created: number; reused: number } {
	const before = iteratorPoolStats();
	for (let i = 0; i < 3; i++) {
		if (txn.getRange().asArray.length !== 1) {
			throw new Error('wrong rows');
		}
	}
	const after = iteratorPoolStats();
	return { created: after.created - before.created, reused: after.reused - before.reused };
}

// Not stalling: pooled, and an idle iterator is left behind in this macrotask.
const txn = new Transaction(db.store);
const notStalling = ranges(txn);
const idleBefore = closables();
// Enabling stalls closes it before config() returns. The main thread blocks
// while the worker does it, so its own pool trim cannot run first.
let worker: Worker | undefined;
if (fromWorker) {
	const signal = new Int32Array(new SharedArrayBuffer(4));
	worker = new Worker(new URL(import.meta.url), { workerData: signal.buffer });
	if (Atomics.wait(signal, 0, 0, 30_000) === 'timed-out') {
		throw new Error('worker did not enable stalls');
	}
} else {
	RocksDatabase.config({ writeBufferManagerAllowStall: true });
}
const idleAfter = closables();
const stalling = ranges(txn);
txn.abort();
if (worker) {
	await once(worker, 'exit');
}
db.close();
console.log(JSON.stringify({ notStalling, stalling, drained: idleBefore - idleAfter }));
