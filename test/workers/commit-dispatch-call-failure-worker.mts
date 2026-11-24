import { RocksDatabase, Transaction } from '../../src/index.ts';
import { workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);
const txn = new Transaction(db.store);
txn.putSync('worker', 1);
void txn.commit();
// The commit's call fails and its completion is released to the finalizer while this thread is
// blocked; closing drops the completion from the descriptor, so only env liveness tells the
// finalizer, which runs during this env's teardown, not to call into JS.
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);
db.close();
process.exit(0);
