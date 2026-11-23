import { RocksDatabase, Transaction } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);
const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
if (workerData.commit) {
	void txn.commit();
}
parentPort?.postMessage({ ready: true });
// Keep this env busy so termination's teardown owns close().
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
