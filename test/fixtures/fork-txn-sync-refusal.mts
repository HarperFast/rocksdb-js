import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { constants, NativeIterator } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// Calls every synchronous transaction entry point through the native transaction, past the JS
// layer's open check, once admission has to refuse it:
// - `closing`: a foreign shutdown() has closed the transaction and the database this worker still
//   holds a handle to.
// - `detached`: the transaction's owner closed its own handle; another handle keeps the database open.
type Mode = 'closing' | 'detached';

async function callEntryPoints(
	db: RocksDatabase,
	txn: Transaction
): Promise<Record<string, string>> {
	const store = db.store;
	const native = (txn as any)._context;
	const outcomes: Record<string, string> = {};
	const calls: Record<string, () => unknown> = {
		getSync: () => store.getSync(native, 'key'),
		get: () => new Promise((resolve, reject) => native.get(Buffer.from('key'), resolve, reject)),
		getMany: () => store.getMany(native, ['key']),
		getManySync: () => store.getManySync(native, ['key']),
		getCount: () => store.getCount(native),
		getRange: () =>
			new NativeIterator(native, constants.ITERATOR_CONTEXT_IS_TRANSACTION_FLAG, 0, 0, 0),
		putSync: () => native.putSync(Buffer.from('key'), Buffer.from('value')),
		removeSync: () => native.removeSync(Buffer.from('key')),
		useLog: () => native.useLog('log', store.db),
		setTimestamp: () => native.setTimestamp(),
		abandonWrites: () => native.abandonWrites(),
	};
	for (const [name, call] of Object.entries(calls)) {
		try {
			await call();
			outcomes[name] = 'ok';
		} catch (error) {
			outcomes[name] = (error as Error).message;
		}
	}
	return outcomes;
}

const mode = (isMainThread ? process.argv[3] : workerData.mode) as Mode;
assert(mode === 'closing' || mode === 'detached');

if (mode === 'detached') {
	const path = process.argv[2];
	assert(path);
	const keeper = RocksDatabase.open(path);
	const owner = RocksDatabase.open(path);
	owner.putSync('key', 'value');
	const txn = new Transaction(owner.store);
	owner.close();
	// Staging through an open handle does not read the closed one's descriptor.
	keeper.putSync('other', 'value', { transaction: txn });
	assert.equal(keeper.getSync('other', { transaction: txn }), 'value');
	const outcomes = await callEntryPoints(owner, txn);
	assert.deepEqual(outcomes, {
		getSync: 'Database not open',
		get: 'Database not open',
		getMany: 'Database not open',
		getManySync: 'Database not open',
		getCount: 'Database not open',
		getRange: 'Database not open',
		putSync: 'Database not open',
		removeSync: 'Database not open',
		useLog: 'Database not open',
		setTimestamp: 'Cannot set timestamp: transaction already has staged writes or log entries',
		abandonWrites: 'ok',
	});
	txn.abort();
	keeper.close();
	assert.equal(registryStatus().length, 0);
	console.log('SUCCESS');
} else if (isMainThread) {
	const path = process.argv[2];
	assert(path);
	const db = RocksDatabase.open(path);
	const worker = new Worker(
		createWorkerBootstrapScript('./test/fixtures/fork-txn-sync-refusal.mts'),
		{
			eval: true,
			workerData: { path, mode },
		}
	);
	try {
		assert.deepEqual(await once(worker, 'message'), ['ready']);
		const result = once(worker, 'message');
		shutdown();
		assert.equal(registryStatus().length, 0);
		worker.postMessage('run');
		// The shutdown closed the transaction, so each call reports it as closed.
		const closed = 'Transaction is closed';
		assert.deepEqual(await result, [
			{
				getSync: closed,
				get: closed,
				getMany: closed,
				getManySync: closed,
				getCount: 'Transaction is not in pending state',
				getRange: 'Transaction is not in pending state',
				putSync: `Operation aborted: ${closed}`,
				removeSync: `Operation aborted: ${closed}`,
				useLog: closed,
				setTimestamp: 'Cannot set timestamp: transaction is not pending',
				abandonWrites: 'ok',
			},
		]);
	} finally {
		await worker.terminate();
		db.close();
	}
	console.log('SUCCESS');
} else {
	const db = RocksDatabase.open(workerData.path);
	db.putSync('key', 'value');
	const txn = new Transaction(db.store);
	txn.getSync('key');
	parentPort!.postMessage('ready');
	await once(parentPort!, 'message');
	parentPort!.postMessage(await callEntryPoints(db, txn));
	parentPort!.close();
}
