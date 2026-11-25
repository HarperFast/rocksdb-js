import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import {
	constants,
	isTransactionCloseDelayedForTesting,
	NativeIterator,
} from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { createHook } from 'node:async_hooks';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

// Calls every synchronous transaction entry point through the native transaction, past the JS
// layer's open check, once admission has to refuse it:
// - `closing`: a foreign shutdown() has closed the transaction and the database this worker still
//   holds a handle to.
// - `cancelling`: a foreign shutdown() is inside the transaction's close(), past its cancellation and
//   before it deletes the transaction (ROCKSDB_JS_TXN_CLOSE_DELAY_MS holds it there).
// - `detached`: the transaction's owner closed its own handle; another handle keeps the database open.
// - `reentrant`: JavaScript that the native call runs itself (an instanceof hook, an option getter,
//   a patched error builder, an async_hooks init hook) aborts the transaction, closes its handle or
//   shuts down; the call must neither wait on its own admission nor dereference the closed handle.
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
	owner.open();
	// The TransactionLog constructor runs after useLog() has bound the store and released its
	// admission; its own instanceof check is the second call to the hook.
	{
		const txn = new Transaction(owner.store);
		let checks = 0;
		Object.defineProperty(NativeDatabase, Symbol.hasInstance, {
			configurable: true,
			value: () => {
				if (++checks === 2) {
					txn.abort();
				}
				return true;
			},
		});
		try {
			assert.ok((txn as any)._context.useLog('log', owner.store.db));
		} finally {
			delete NativeDatabase[Symbol.hasInstance];
		}
		assert.equal(checks, 2);
		assert.equal(owner.useLog('log').getStats().pendingTransactions, 0);
		const logDir = join(path, 'transaction_logs', 'log');
		assert.ok(existsSync(logDir));
		owner.purgeLogs({ destroy: true });
		assert.ok(!existsSync(logDir), 'a pending transaction kept the log store open');
	}

	const whileBuildingError = (hook: () => void, call: () => unknown) => {
		const create = Object.create;
		let pending = true;
		Object.create = ((proto: object | null, properties?: PropertyDescriptorMap) => {
			if (pending && proto === Error.prototype) {
				pending = false;
				hook();
			}
			return create(proto, properties as PropertyDescriptorMap);
		}) as typeof Object.create;
		try {
			call();
			return 'ok';
		} catch (error) {
			return (error as Error).message;
		} finally {
			Object.create = create;
			assert.equal(pending, false, 'no error was built');
		}
	};
	const whileCreatingWork = async (type: string, hook: () => void, call: () => unknown) => {
		let pending = true;
		const asyncHook = createHook({
			init(_asyncId, resourceType) {
				if (pending && resourceType === type) {
					pending = false;
					hook();
				}
			},
		}).enable();
		try {
			await call();
			return 'ok';
		} catch (error) {
			return (error as Error).message;
		} finally {
			asyncHook.disable();
			assert.equal(pending, false, `no ${type} work was created`);
		}
	};
	const nativeGet = (native: any) =>
		new Promise((resolve, reject) => native.get(Buffer.from('key'), resolve, reject));
	const nativeGetMany = (context: any, txnId?: number) =>
		new Promise((resolve, reject) =>
			context.getMany(Buffer.from('key'), new Int32Array([3]), 1, resolve, reject, txnId)
		);
	const staged = () => {
		const txn = new Transaction(owner.store);
		txn.putSync('staged', 'value');
		return txn;
	};
	const frozen = 'Cannot set timestamp: transaction already has staged writes or log entries';
	{
		const txn = staged();
		assert.equal(
			whileBuildingError(
				() => {
					throw new Error('sentinel');
				},
				() => (txn as any)._context.setTimestamp(1)
			),
			'sentinel'
		);
		txn.abort();
	}
	{
		const txn = staged();
		assert.equal(
			whileBuildingError(
				() => txn.abort(),
				() => (txn as any)._context.setTimestamp(1)
			),
			frozen
		);
	}
	{
		const txn = new Transaction(owner.store);
		assert.match(
			whileBuildingError(
				() => txn.abort(),
				() => (txn as any)._context.setTimestamp(-1)
			),
			/^Invalid timestamp/
		);
	}
	const dropped = RocksDatabase.open(path, { name: 'dropped' });
	const putTxn = new Transaction(dropped.store);
	const removeTxn = new Transaction(dropped.store);
	dropped.dropSync();
	assert.match(
		whileBuildingError(
			() => putTxn.abort(),
			() => (putTxn as any)._context.putSync(Buffer.from('key'), Buffer.from('value'))
		),
		/dropped/
	);
	assert.match(
		whileBuildingError(
			() => removeTxn.abort(),
			() => (removeTxn as any)._context.removeSync(Buffer.from('key'))
		),
		/dropped/
	);
	dropped.close();
	// Bun emits async_hooks events for process.nextTick only, not for N-API async work.
	const emitsNapiInit = !process.versions.bun && !('deno' in process.versions);
	if (emitsNapiInit) {
		for (const [type, read] of [
			['transaction.get', (txn: Transaction) => nativeGet((txn as any)._context)],
			['transaction.getMany', (txn: Transaction) => nativeGetMany((txn as any)._context)],
			[
				'transaction.get',
				(txn: Transaction) =>
					new Promise((resolve, reject) =>
						(owner.store.db as any).get(Buffer.from('key'), resolve, reject, txn.id)
					),
			],
			['transaction.getMany', (txn: Transaction) => nativeGetMany(owner.store.db, txn.id)],
		] as const) {
			const txn = new Transaction(owner.store);
			assert.equal(
				await whileCreatingWork(
					type,
					() => txn.abort(),
					() => read(txn)
				),
				'Transaction is closed',
				type
			);
		}
		// The read owns its keys before creating its work: a hook that reuses the shared key buffers
		// must not change which records it returns.
		owner.putSync('a', 'value-a');
		owner.putSync('b', 'value-b');
		{
			const txn = new Transaction(owner.store);
			const keyLength = owner.store.encodeKey('a').end;
			let value: Buffer | undefined;
			assert.equal(
				await whileCreatingWork(
					'transaction.get',
					() => owner.getSync('b'),
					async () => {
						value = (await new Promise((resolve, reject) =>
							(txn as any)._context.get(keyLength, resolve, reject)
						)) as Buffer;
					}
				),
				'ok'
			);
			assert.ok(value?.toString().includes('value-a'), String(value));
			txn.abort();
		}
		{
			const txn = new Transaction(owner.store);
			const keys = Buffer.from('ab');
			let values: Buffer | undefined;
			assert.equal(
				await whileCreatingWork(
					'transaction.getMany',
					() => keys.fill('z'),
					async () => {
						values = (await new Promise((resolve, reject) =>
							(txn as any)._context.getMany(keys, new Int32Array([1, 2]), 2, resolve, reject)
						)) as Buffer;
					}
				),
				'ok'
			);
			assert.ok(values?.toString().includes('value-a'), String(values));
			assert.ok(values?.toString().includes('value-b'), String(values));
			txn.abort();
		}
		// A read refused after its hook closed the owner's handle must not have pinned a snapshot.
		{
			const txn = new Transaction(owner.store);
			assert.equal(
				await whileCreatingWork(
					'transaction.get',
					() => owner.close(),
					() => nativeGet((txn as any)._context)
				),
				'Database not open'
			);
			keeper.putSync('after', 'value');
			assert.equal(keeper.getSync('after', { transaction: txn }), 'value');
			txn.abort();
			owner.open();
		}
	}
	keeper.close();

	// Teardown re-entered from the same JavaScript: shutdown() closes the transaction under the call.
	{
		const txn = staged();
		assert.equal(
			whileBuildingError(shutdown, () => (txn as any)._context.setTimestamp(1)),
			frozen
		);
		assert.equal(registryStatus().length, 0);
	}
	if (emitsNapiInit) {
		owner.open();
		const txn = new Transaction(owner.store);
		assert.equal(
			await whileCreatingWork('transaction.getMany', shutdown, () =>
				nativeGetMany((txn as any)._context)
			),
			'Transaction is closed'
		);
	}
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
