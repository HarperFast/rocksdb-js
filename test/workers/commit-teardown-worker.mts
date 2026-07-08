import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

// Open the SAME path as the parent so the parent's handle pins the shared
// DBDescriptor (and its commit thread) for the whole run. When this worker's
// env is terminated, the descriptor — and its commit thread — survive on the
// parent, so the commit thread can still try to complete this worker's queued
// commits back into a torn-down env. That is the lifecycle the test exercises.
const db = RocksDatabase.open(workerData.path);

// Fire a bounded batch of async commits WITHOUT awaiting them, so a fixed
// number are queued/executing on the shared commit thread. With
// ROCKSDB_JS_COMMIT_DELAY_MS set, each completion is delayed on the commit
// thread, so all of these are still in flight when the parent terminates this
// env a few ms later. Bounded (not a continuous flood) so teardown and the
// parent's final db.close() drain quickly.
const IN_FLIGHT = 16;
for (let i = 0; i < IN_FLIGHT; i++) {
	db.transaction((txn) => {
		txn.put(`k${i}`, i);
	}).catch(() => {
		// the env/db may be tearing down as the worker is terminated; ignore
	});
}

// The in-flight commits ref the completion tsfn, keeping this env's loop alive
// until the parent terminates it.
parentPort?.postMessage({ ready: true });
