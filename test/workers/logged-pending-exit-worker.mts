import { RocksDatabase, Transaction } from '../../src/index.ts';
import { forceTryAgainForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Logged-pending worker requires a parent port');
const port = parentPort;

// Leaves a transaction whose log batch is durable but whose commit never landed
// pending when this env exits, so the env-cleanup reap closes it.
const db = RocksDatabase.open(workerData.path);
const log = db.useLog(workerData.log);
const txn = new Transaction(db.store);
try {
	txn.putSync(workerData.key, 'from-worker');
	log.addEntry(Buffer.from(workerData.key), txn.id);
	forceTryAgainForTesting(1);
	let code: string | undefined;
	try {
		await txn.commit();
	} catch (error) {
		code = (error as { code?: string }).code;
	}
	port.postMessage({ code, hasDurableLog: txn.hasDurableLog });
} catch (error) {
	port.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
// The handle is deliberately left open; the worker exits with it pending.
