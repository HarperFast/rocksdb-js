import { RocksDatabase, Transaction } from '../../src/index.ts';
import { workerData } from 'node:worker_threads';

const inFlight = workerData.mode === 'worker-exit-in-flight';
if (inFlight) {
	// One lane, so the second commit is still executing when the first one's call fails.
	RocksDatabase.config({ commitThreads: 1 });
}
const db = RocksDatabase.open(workerData.path);
const commit = (key: string) => {
	const txn = new Transaction(db.store);
	txn.putSync(key, 1);
	void txn.commit();
};
commit('worker');
if (inFlight) {
	commit('worker2');
}
// `worker-exit`: the failed completion is released while this thread is blocked, so on Node its
// finalizer runs before this env's cleanup hook and settles into the exiting env.
// `worker-exit-in-flight`: the second commit keeps the retired completion from being released
// before exit, so its finalizer runs after the hook marked this env gone.
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, inFlight ? 400 : 500);
if (!inFlight) {
	db.close();
}
process.exit(0);
