import { RocksDatabase, shutdown, type Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// The worker parks right after beforecommit passes, while this thread's shutdown() aborts its
// transaction, so the native commit finds it already aborted.
if (isMainThread) {
	const [path, method] = process.argv.slice(2);
	assert(path);
	assert(method === 'transaction' || method === 'transactionSync');
	const gate = new Int32Array(new SharedArrayBuffer(8));
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-commit-foreign-abort.mts'),
		{ eval: true, workerData: { path, method, gate } }
	);
	try {
		let replied = false;
		const result = once(worker, 'message').finally(() => (replied = true));
		const deadline = Date.now() + 5000;
		// A worker that fails before beforecommit replies instead, and the assertion below shows why.
		while (Atomics.load(gate, 0) !== 1 && !replied) {
			assert(Date.now() < deadline, 'the worker never reached beforecommit');
			await delay(1);
		}
		if (!replied) {
			shutdown();
			Atomics.store(gate, 1, 1);
			Atomics.notify(gate, 1);
		}
		assert.deepEqual(await result, [{ threw: true, message: 'Database is closing' }]);
	} finally {
		await worker.terminate();
	}
	const reopened = RocksDatabase.open(path);
	try {
		assert.equal(reopened.getSync('k'), undefined);
	} finally {
		reopened.close();
	}
	console.log('SUCCESS');
} else {
	const { path, method, gate } = workerData;
	const db = RocksDatabase.open(path);
	const body = (txn: Transaction) => {
		txn.putSync('k', 'v');
		const notify = txn.notify.bind(txn);
		txn.notify = (event: string, ...args: any[]) => {
			const notified = notify(event, ...args);
			if (event === 'beforecommit') {
				Atomics.store(gate, 0, 1);
				Atomics.wait(gate, 1, 0, 10000);
			}
			return notified;
		};
		return 'returned';
	};
	try {
		const returned =
			method === 'transaction' ? await db.transaction(body) : db.transactionSync(body);
		parentPort!.postMessage({ threw: false, returned });
	} catch (error) {
		parentPort!.postMessage({ threw: true, message: (error as Error).message });
	}
	parentPort!.close();
}
