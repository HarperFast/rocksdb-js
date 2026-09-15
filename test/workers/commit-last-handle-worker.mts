import { RocksDatabase, Transaction } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath } = workerData as { dbPath: string };
const db = RocksDatabase.open(dbPath);
const txn = new Transaction(db.store);
txn.putSync('committed', 'value');
void txn.commit().catch(() => {});

const deadline = Date.now() + 5000;
while (!isTransactionCommitExecuteDelayedForTesting()) {
	if (Date.now() >= deadline) {
		throw new Error('commit did not reach the native execute delay');
	}
	await new Promise((resolve) => setTimeout(resolve, 5));
}
parentPort?.postMessage('delayed');
await new Promise(() => {});
