/**
 * Run with ROCKSDB_JS_MEMORY_MAP_BUFFER_DELAY_MS and --expose-gc: a worker terminated while
 * `_getMemoryMapOfFile()` holds its mapping reference makes the JS buffer create fail, and the
 * mapping must still be released.
 */
import { RocksDatabase } from '../../src/index.ts';
import { transactionLogMapCount } from '../../src/load-binding.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const fail = (message: string): never => {
	throw new Error(message);
};

if (!isMainThread) {
	const db = RocksDatabase.open(workerData.dbPath, { transactionLogMaxSize: 1000 });
	const log = db.useLog('foo');
	parentPort!.once('message', () => {
		let outcome: string;
		try {
			outcome = log._getMemoryMapOfFile(1) ? 'buffer' : 'none';
		} catch {
			outcome = 'threw';
		}
		parentPort!.postMessage(outcome);
	});
	parentPort!.postMessage('ready');
} else {
	const dbPath = process.argv[2];
	const delayMs = Number(process.env.ROCKSDB_JS_MEMORY_MAP_BUFFER_DELAY_MS);
	if (!(delayMs > 0)) fail('run with ROCKSDB_JS_MEMORY_MAP_BUFFER_DELAY_MS');
	const gc = globalThis.gc ?? fail('run with --expose-gc');
	const until = async (condition: () => boolean, what: string): Promise<void> => {
		const deadline = performance.now() + 5000;
		while (!condition()) {
			if (performance.now() > deadline) fail(`timed out waiting for ${what}`);
			gc();
			await delay(10);
		}
	};
	const db = RocksDatabase.open(dbPath, { transactionLogMaxSize: 1000 });
	try {
		const log = db.useLog('foo');
		const value = Buffer.alloc(100, 'a');
		// Rotates past segment 1, which then holds only a weak reference to its mapping.
		for (let i = 0; i < 20; i++) {
			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});
		}

		const worker = new Worker(new URL(import.meta.url), { workerData: { dbPath } });
		const messages: string[] = [];
		worker.on('message', (message: string) => messages.push(message));
		await until(() => messages.includes('ready'), 'the worker to open the database');
		gc();
		await delay(50);
		const before = transactionLogMapCount();

		const goAt = performance.now();
		worker.postMessage('go');
		await until(() => transactionLogMapCount() === before + 1, 'the worker to map segment 1');
		// The worker's delay starts after it receives 'go', so this proves terminate() lands inside it.
		if (performance.now() - goAt > delayMs / 2) fail('too slow to terminate inside the delay');
		await worker.terminate();
		if (messages.includes('buffer')) fail('terminate() landed after the buffer was created');

		await until(() => transactionLogMapCount() === before, 'the mapping to be released');
		console.log(JSON.stringify({ released: true }));
	} finally {
		db.close();
	}
}
