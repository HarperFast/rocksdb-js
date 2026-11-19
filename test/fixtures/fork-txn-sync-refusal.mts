import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import {
	constants,
	isTransactionCloseDelayedForTesting,
	NativeIterator,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// Calls every synchronous transaction entry point through the native transaction, past the JS
// layer's open check, once admission has to refuse it:
// - `closing`: a foreign shutdown() has closed the transaction and the database this worker still
//   holds a handle to.
// - `cancelling`: a foreign shutdown() is inside the transaction's close(), past its cancellation and
//   before it deletes the transaction (ROCKSDB_JS_TXN_CLOSE_DELAY_MS holds it there).
// - `detached`: the transaction's owner closed its own handle; another handle keeps the database open.
// - `reentrant`: JavaScript that the native call runs itself aborts the transaction or closes its
//   handle; the call must neither wait on its own admission nor dereference the closed handle.
type Mode = 'closing' | 'cancelling' | 'detached' | 'reentrant';

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
assert(['closing', 'cancelling', 'detached', 'reentrant'].includes(mode));

if (mode === 'reentrant') {
	const path = process.argv[2];
	assert(path);
	const keeper = RocksDatabase.open(path);
	const owner = RocksDatabase.open(path);
	const NativeDatabase = (owner.store.db as any).constructor;
	const useLogWhile = (hook: () => void) => {
		const txn = new Transaction(owner.store);
		Object.defineProperty(NativeDatabase, Symbol.hasInstance, {
			configurable: true,
			value: () => {
				hook.call(txn);
				return true;
			},
		});
		try {
			(txn as any)._context.useLog('log', owner.store.db);
			return 'ok';
		} catch (error) {
			return (error as Error).message;
		} finally {
			delete NativeDatabase[Symbol.hasInstance];
		}
	};
	assert.equal(
		useLogWhile(function (this: Transaction) {
			this.abort();
		}),
		'Transaction is closed'
	);
	assert.equal(
		useLogWhile(() => owner.close()),
		'Database not open'
	);
	owner.open();
	assert.throws(
		() =>
			new Transaction(owner.store, {
				get disableSnapshot() {
					owner.close();
					return false;
				},
			} as any),
		{ message: 'Database not open' }
	);
	keeper.close();
	assert.equal(registryStatus().length, 0);
	console.log('SUCCESS');
} else if (mode === 'detached') {
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
		if (mode === 'cancelling') {
			worker.postMessage('run');
		}
		shutdown();
		assert.equal(registryStatus().length, 0);
		if (mode === 'closing') {
			worker.postMessage('run');
		}
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
				// Mid-close the transaction is still pending, so only admission refuses it.
				setTimestamp:
					mode === 'cancelling' ? closed : 'Cannot set timestamp: transaction is not pending',
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
	if (workerData.mode === 'cancelling') {
		const deadline = Date.now() + 5000;
		while (!isTransactionCloseDelayedForTesting()) {
			assert(Date.now() < deadline, 'the foreign close never reached its delay');
		}
	}
	parentPort!.postMessage(await callEntryPoints(db, txn));
	parentPort!.close();
}
