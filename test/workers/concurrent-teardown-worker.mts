import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

// Opens the SAME database path as its sibling workers (passed via workerData),
// then waits at a barrier until every sibling has also opened before doing any
// work or exiting. The barrier guarantees all workers share the one
// process-global DBDescriptor simultaneously and then tear it down at roughly
// the same time — concurrent DBRegistry::CloseDB on a shared descriptor, which
// is the race that used to corrupt the heap. (Without the barrier a fast worker
// could fully purge the descriptor, releasing RocksDB's per-path LOCK, while a
// slow worker was still in open() — a separate reopen/lock-contention pattern
// the benchmarks never hit, since their workers open once at setup and tear
// down together at the end.)
const { path, puts, close, readyBuf, total } = workerData;
const ready = new Int32Array(readyBuf);

const db = RocksDatabase.open(path, { disableWAL: true });

// Barrier: signal we have opened, then wait until all `total` workers have.
Atomics.add(ready, 0, 1);
Atomics.notify(ready, 0);
let seen = Atomics.load(ready, 0);
while (seen < total) {
	Atomics.wait(ready, 0, seen, 200);
	seen = Atomics.load(ready, 0);
}

const payload = Buffer.alloc(64, 'x');
for (let i = 0; i < puts; i++) {
	db.putSync(`key-${(Math.random() * 5000) | 0}`, payload);
}

// Half the workers close explicitly and half exit with the database still open,
// so the env-cleanup hook tears down the shared descriptor concurrently.
if (close) {
	db.close();
}

parentPort?.postMessage({ done: true });
process.exit(0);
