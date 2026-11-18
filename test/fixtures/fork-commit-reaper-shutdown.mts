import { RocksDatabase, shutdown } from '../../src/index.ts';
import {
	isTransactionCloseDelayedForTesting,
	isTransactionCloseGuardedAtDrainForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { once } from 'node:events';
import { Worker } from 'node:worker_threads';

if (!(Number(process.env.ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH) > 0)) {
	throw new Error('ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH must be set');
}
// The env cleanup hook's reaper closes a pending transaction. On Node a committed one's queued
// completion runs during env teardown, before that hook, and closes it first.
const path = process.argv[2];
const state = process.argv[3];
const closer = process.argv[4];
if (state !== 'committed' && state !== 'pending') throw new Error(`unknown state: ${state}`);
if (closer !== 'shutdown' && closer !== 'destroy') throw new Error(`unknown closer: ${closer}`);
const db = RocksDatabase.open(path);
const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/commit-reaper-shutdown-worker.mts'),
	{ eval: true, workerData: { path, commit: state === 'committed' } }
);
await once(worker, 'message');

const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const deadline = Date.now() + 10_000;
// Let native execution land the write before termination can cancel it.
while (state === 'committed' && db.getSync('k') !== 'committed') {
	if (Date.now() > deadline) throw new Error('worker commit never landed');
	Atomics.wait(sleepCell, 0, 0, 2);
}
const terminated = worker.terminate();
while (!isTransactionCloseDelayedForTesting()) {
	if (Date.now() > deadline) throw new Error('worker teardown never reached the close latch');
	Atomics.wait(sleepCell, 0, 0, 2);
}
if (closer === 'destroy') {
	db.destroy();
} else {
	shutdown();
}
if (!isTransactionCloseGuardedAtDrainForTesting()) {
	throw new Error(
		`${closer}() reached the drain without the ${state} transaction's close operation registered`
	);
}
await terminated;

if (closer === 'shutdown') {
	const reopened = RocksDatabase.open(path);
	try {
		const expected = state === 'committed' ? 'committed' : undefined;
		const value = reopened.getSync('k');
		if (value !== expected) throw new Error(`expected ${expected} after shutdown, read ${value}`);
	} finally {
		reopened.close();
	}
}
