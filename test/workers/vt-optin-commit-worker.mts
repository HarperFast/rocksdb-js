import { RocksDatabase, Transaction } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path, { encoding: false });
const plain = RocksDatabase.open(workerData.path, { name: 'cached', encoding: false });
const txn = new Transaction(db.store);
plain.putSync(Buffer.from('staged'), Buffer.from('v'), { transaction: txn });
// Held before RocksDB applies it by ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS; the parent terminates
// this env meanwhile.
txn.commit().catch(() => {});
plain.close();
parentPort?.postMessage('staged');
