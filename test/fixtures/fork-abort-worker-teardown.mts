import { RocksDatabase, Transaction } from '../../src/index.ts';
import { lockWakeCallbackCount } from '../../src/load-binding.ts';
import { parseTransactionLog } from '../../src/parse-transaction-log.ts';
import { RETRY_NOW } from '../../src/transaction.ts';
import { join } from 'node:path';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// A worker terminated while its logged retry is parked on an older transaction's write intent:
// teardown is a terminal abort, so the batch is marked, the watermark released and intents freed.
const fail = (message: string): never => {
	throw new Error(message);
};
const message = (port: { once: (event: 'message', cb: (value: string) => void) => void }) =>
	new Promise<string>((resolve) => port.once('message', resolve));

RocksDatabase.config({ transactionLogAbortMarkers: true });

if (!isMainThread) {
	const db = RocksDatabase.open(workerData.dbPath, { verificationTable: true });
	const log = db.useLog('foo');
	const txn = new Transaction(db.store, { coordinatedRetry: true });
	log.addEntry(Buffer.from('worker'), txn.id);
	await txn.put('hot', 'worker');
	parentPort!.postMessage('staged');
	await message(parentPort!); // main has committed a conflicting write
	if ((await txn.commit()) !== RETRY_NOW || !txn.logWritten()) {
		fail('expected a logged conflict');
	}
	parentPort!.postMessage('logged');
	await message(parentPort!); // main's older transaction now holds the key
	await txn.put('hot', 'worker-retry');
	void txn.commit();
	parentPort!.postMessage('parked');
	await new Promise(() => {});
} else {
	const dbPath = process.argv[2];
	const db = RocksDatabase.open(dbPath, { verificationTable: true });
	try {
		await db.put('hot', 'v0');
		db.populateVersion('hot', 1.5e12);
		const older = new Transaction(db.store);
		const worker = new Worker(new URL(import.meta.url), { workerData: { dbPath } });
		if ((await message(worker)) !== 'staged') fail('worker did not stage');
		await db.put('hot', 'conflict');
		worker.postMessage('go');
		if ((await message(worker)) !== 'logged') fail('worker did not log');
		await older.put('hot', 'older');
		worker.postMessage('go');
		if ((await message(worker)) !== 'parked') fail('worker did not park');
		// parked once its wake callback is registered on the older transaction's intent
		const deadline = Date.now() + 5000;
		while (lockWakeCallbackCount() === 0) {
			if (Date.now() > deadline) fail('worker commit never parked');
			await new Promise((resolve) => setTimeout(resolve, 5));
		}
		await worker.terminate();

		const flags = parseTransactionLog(
			join(dbPath, 'transaction_logs', 'foo', '1.txnlog')
		).entries.map((entry) => entry.flags);
		if (JSON.stringify(flags) !== JSON.stringify([3])) {
			fail(`expected the worker's batch marked aborted, got flags ${JSON.stringify(flags)}`);
		}
		older.abort();
		if (lockWakeCallbackCount() !== 0) {
			fail(`wake callbacks left registered: ${lockWakeCallbackCount()}`);
		}
		const log = db.useLog('foo');
		const later = new Transaction(db.store);
		log.addEntry(Buffer.from('later'), later.id);
		await later.put('hot', 'later');
		await later.commit();
		const entries = Array.from(log.query({ start: 0 }), (entry) => entry.data.toString());
		if (JSON.stringify(entries) !== JSON.stringify(['later'])) {
			fail(`readers saw ${JSON.stringify(entries)}`);
		}
	} finally {
		db.close();
	}
}
