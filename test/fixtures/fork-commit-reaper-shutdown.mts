import { RocksDatabase, shutdown } from '../../src/index.ts';
import {
	isTransactionCloseDelayedForTesting,
	isTransactionCloseAwaitedByShutdownForTesting,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { once } from 'node:events';
import { Worker } from 'node:worker_threads';

if (!(Number(process.env.ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH) > 0)) {
	throw new Error('ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH must be set');
}
const path = process.argv[2];
const db = RocksDatabase.open(path);
const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/commit-reaper-shutdown-worker.mts'),
	{ eval: true, workerData: { path } }
);
await once(worker, 'message');

const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const deadline = Date.now() + 10_000;
// Let native execution land the write before termination can cancel it.
while (db.getSync('k') !== 'committed') {
	if (Date.now() > deadline) throw new Error('worker commit never landed');
	Atomics.wait(sleepCell, 0, 0, 2);
}
const terminated = worker.terminate();
while (!isTransactionCloseDelayedForTesting()) {
	if (Date.now() > deadline) throw new Error('worker reaper never reached the close latch');
	Atomics.wait(sleepCell, 0, 0, 2);
}
shutdown();
if (!isTransactionCloseAwaitedByShutdownForTesting()) {
	throw new Error("shutdown() did not wait for the reaper's close");
}
await terminated;

const reopened = RocksDatabase.open(path);
try {
	if (reopened.getSync('k') !== 'committed') throw new Error('commit did not survive shutdown');
} finally {
	reopened.close();
}
