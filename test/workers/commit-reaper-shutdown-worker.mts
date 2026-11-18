import { RocksDatabase, Transaction } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);
const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
void txn.commit();
parentPort?.postMessage({ ready: true });
// Keep the completion queued on this env so termination's reaper owns close().
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
