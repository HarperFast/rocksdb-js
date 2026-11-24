import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { existsSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

// Holds native getCount() calls open in options getters. Database getCount()
// reads its options after admission; transaction getCount() reads them before.

const DESTROY = 0;
const HOLDER = 1;
const RELEASE = 2;

const path = process.argv[2];
const mode = process.argv[3];

function holdInGetCount(native: any, getter: () => void): unknown {
	try {
		native.getCount(
			{
				get exclusiveStart() {
					getter();
					return false;
				},
			},
			undefined
		);
	} catch (error) {
		return error;
	}
}

function startWorker(state: Int32Array) {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/sync-operation-destroy-worker.mts'),
		{ eval: true, workerData: { path, state: state.buffer } }
	);
	const ready = Promise.withResolvers<void>();
	const done = Promise.withResolvers<any>();
	worker.on('error', (error) => {
		ready.reject(error);
		done.reject(error);
	});
	worker.on('message', (message) => (message.ready ? ready.resolve() : done.resolve(message)));
	return { worker, ready: ready.promise, done: done.promise };
}

function waitUntil(state: Int32Array, index: number, from: number, what: string) {
	if (Atomics.wait(state, index, from, 5_000) === 'timed-out') {
		throw new Error(`Timed out waiting for ${what}`);
	}
}

if (mode === 'destroy' || mode === 'destroy-holder-last') {
	const db = RocksDatabase.open(path);
	db.putSync('key', 'value');
	const native = (db as any).store.db;
	const state = new Int32Array(new SharedArrayBuffer(12));
	const holder = startWorker(state);
	const destroyer = startWorker(state);
	await Promise.all([holder.ready, destroyer.ready]);

	holder.worker.postMessage('hold');
	waitUntil(state, HOLDER, 0, 'the second handle to enter its call');

	// Each order leaves one handle alone holding the destroy for the 300 ms
	// check, so a drain that skipped either handle fails one of them.
	const releaseHolder = () => {
		Atomics.store(state, RELEASE, 1);
		Atomics.notify(state, RELEASE);
		waitUntil(state, HOLDER, 1, 'the second handle to leave its call');
	};
	let observed = '';
	const callError = holdInGetCount(native, () => {
		destroyer.worker.postMessage('destroy');
		waitUntil(state, DESTROY, 0, 'destroy to start');
		const deadline = Date.now() + 5_000;
		while (native.opened) {
			if (Date.now() > deadline) throw new Error('destroy never published closing');
			Atomics.wait(state, DESTROY, 1, 5);
		}
		if (mode === 'destroy') {
			releaseHolder();
			observed =
				Atomics.wait(state, DESTROY, 1, 300) === 'timed-out'
					? 'waited'
					: 'destroy returned while the main thread was inside a synchronous call';
		} else {
			observed = 'waited';
		}
		// Reopening would replace the descriptor this call borrows, and would
		// wait for the destroy that is waiting for this call.
		try {
			db.open();
			observed = 'reopened inside a synchronous call';
		} catch (error) {
			if (!String(error).includes('Cannot open the database during one of its own operations')) {
				observed = `unexpected reopen error: ${String(error)}`;
			}
		}
	});
	if (observed !== 'waited') throw new Error(observed || String(callError));
	if (!String(callError).includes('Database is closing')) {
		throw new Error(`Expected the scan to observe the close, got ${String(callError)}`);
	}

	if (mode === 'destroy-holder-last') {
		if (Atomics.wait(state, DESTROY, 1, 300) !== 'timed-out') {
			throw new Error('destroy returned while the second handle was inside a synchronous call');
		}
		releaseHolder();
	}

	const held = await holder.done;
	if (!String(held.error).includes('Database is closing')) {
		throw new Error(`Expected the held scan to observe the close, got ${JSON.stringify(held)}`);
	}
	const result = await destroyer.done;
	if (!result.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(result)}`);
	if (registryStatus().some((entry) => entry.path === path)) {
		throw new Error('Expected destroy to fully clear the registry entry');
	}
	await Promise.all([holder.worker.terminate(), destroyer.worker.terminate()]);
} else if (mode === 'reentrant') {
	const db = RocksDatabase.open(path);
	db.putSync('key', 'value');
	const native = (db as any).store.db;
	for (const action of ['close', 'destroy'] as const) {
		let refusal: unknown;
		const callError = holdInGetCount(native, () => {
			try {
				db[action]();
			} catch (error) {
				refusal = error;
			}
		});
		if (callError) throw callError;
		if (
			!String(refusal).includes(`Cannot ${action} the database during one of its own operations`)
		) {
			throw new Error(`Expected the re-entrant ${action} to be refused, got ${String(refusal)}`);
		}
		if (db.getSync('key') !== 'value')
			throw new Error(`Database unusable after a refused ${action}`);
	}
	db.close();
	if (registryStatus().some((entry) => entry.path === path)) {
		throw new Error('Expected close to clear the registry entry');
	}
} else if (mode === 'reentrant-other') {
	const db = RocksDatabase.open(path);
	const column = RocksDatabase.open(path, { name: 'column' });
	const elsewherePath = `${path}-elsewhere`;
	const elsewhere = RocksDatabase.open(elsewherePath);
	db.putSync('key', 'value');
	column.putSync('key', 'value');
	let refusal: unknown;
	const callError = holdInGetCount((db as any).store.db, () => {
		try {
			column.destroy();
		} catch (error) {
			refusal = error;
		}
		elsewhere.destroy();
	});
	if (callError) throw callError;
	if (!String(refusal).includes('Cannot destroy the database during one of its own operations')) {
		throw new Error(`Expected the re-entrant destroy to be refused, got ${String(refusal)}`);
	}
	if (existsSync(elsewherePath)) {
		throw new Error('Expected a database this call does not use to be destroyed');
	}
	if (db.getSync('key') !== 'value' || column.getSync('key') !== 'value') {
		throw new Error('Database unusable after a refused destroy');
	}
	column.destroy();
	if (db.isOpen() || registryStatus().some((entry) => entry.path === path)) {
		throw new Error('Expected destroy to close every handle and clear the registry entry');
	}
} else if (mode === 'reentrant-foreign-closing') {
	RocksDatabase.config({ lifecycleWaitSeconds: 30 });
	const db = RocksDatabase.open(path);
	const column = RocksDatabase.open(path, { name: 'column' });
	db.putSync('key', 'value');
	const native = (db as any).store.db;
	const state = new Int32Array(new SharedArrayBuffer(12));
	const destroyer = startWorker(state);
	await destroyer.ready;
	const refusals: string[] = [];
	holdInGetCount(native, () => {
		destroyer.worker.postMessage('destroy');
		waitUntil(state, DESTROY, 0, 'destroy to start');
		const deadline = Date.now() + 5_000;
		while (native.opened) {
			if (Date.now() > deadline) throw new Error('destroy never published closing');
			Atomics.wait(state, DESTROY, 1, 5);
		}
		for (const teardown of [() => column.destroy(), () => shutdown()]) {
			const started = Date.now();
			try {
				teardown();
				refusals.push('ran');
			} catch (error) {
				refusals.push(`${String(error)} after ${Date.now() - started} ms`);
			}
		}
	});
	const expected = [
		'Cannot destroy the database during one of its own operations',
		'Cannot shut down during a database operation on this thread',
	];
	if (refusals.length !== expected.length) {
		throw new Error(`Expected ${expected.length} refusals, got ${JSON.stringify(refusals)}`);
	}
	refusals.forEach((refusal, i) => {
		const elapsed = Number(/after (\d+) ms/.exec(refusal)?.[1] ?? Infinity);
		if (!refusal.includes(expected[i]) || elapsed > 1_000) {
			throw new Error(`Expected a prompt refusal, got ${refusals.join('; ')}`);
		}
	});
	const result = await destroyer.done;
	if (!result.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(result)}`);
	await destroyer.worker.terminate();
} else if (mode === 'reentrant-shutdown') {
	const db = RocksDatabase.open(path);
	const other = RocksDatabase.open(`${path}-other`);
	db.putSync('key', 'value');
	other.putSync('key', 'value');
	const listener = () => {};
	RocksDatabase.on('reentrant-shutdown', listener);
	let refusal: unknown;
	const callError = holdInGetCount((db as any).store.db, () => {
		try {
			shutdown();
		} catch (error) {
			refusal = error;
		}
	});
	if (callError) throw callError;
	if (!String(refusal).includes('Cannot shut down during a database operation on this thread')) {
		throw new Error(`Expected the re-entrant shutdown to be refused, got ${String(refusal)}`);
	}
	if (db.getSync('key') !== 'value' || other.getSync('key') !== 'value') {
		throw new Error('Database unusable after a refused shutdown');
	}
	if (RocksDatabase.listenerCount('reentrant-shutdown') !== 1) {
		throw new Error('A refused shutdown released the global listeners');
	}
	shutdown();
	if (db.isOpen() || other.isOpen() || registryStatus().length !== 0) {
		throw new Error('Expected shutdown to close every database');
	}
	const reopened = RocksDatabase.open(path);
	if (reopened.getSync('key') !== 'value') throw new Error('Expected the data to survive shutdown');
	reopened.destroy();
	RocksDatabase.open(`${path}-other`).destroy();
} else if (mode === 'foreign-shutdown') {
	const db = RocksDatabase.open(path);
	db.putSync('key', 'value');
	const native = (db as any).store.db;
	const state = new Int32Array(new SharedArrayBuffer(12));
	const shutter = startWorker(state);
	await shutter.ready;
	let observed = '';
	const callError = holdInGetCount(native, () => {
		shutter.worker.postMessage('shutdown');
		waitUntil(state, DESTROY, 0, 'shutdown to start');
		const deadline = Date.now() + 5_000;
		while (native.opened) {
			if (Date.now() > deadline) throw new Error('shutdown never published closing');
			Atomics.wait(state, DESTROY, 1, 5);
		}
		observed =
			Atomics.wait(state, DESTROY, 1, 300) === 'timed-out'
				? 'waited'
				: 'shutdown returned while the main thread was inside a synchronous call';
	});
	if (observed !== 'waited') throw new Error(observed || String(callError));
	if (!String(callError).includes('Database is closing')) {
		throw new Error(`Expected the scan to observe the close, got ${String(callError)}`);
	}
	const result = await shutter.done;
	if (!result.destroyed) throw new Error(`Shutdown failed: ${JSON.stringify(result)}`);
	await shutter.worker.terminate();
} else if (mode === 'reentrant-txn') {
	const db = RocksDatabase.open(path);
	const column = RocksDatabase.open(path, { name: 'column' });
	db.putSync('key', 'value');
	let destroyed = false;
	let callError: unknown;
	try {
		db.transactionSync((txn) => {
			try {
				(txn as any)._context.getCount({
					get exclusiveStart() {
						column.destroy();
						destroyed = true;
						return false;
					},
				});
			} catch (error) {
				callError = error;
			}
		});
	} catch {
		// The destroy closed the transaction, so its commit fails too.
	}
	if (!destroyed) throw new Error(`Expected the destroy to run, got ${String(callError)}`);
	if (!callError) throw new Error('Expected getCount() to fail on the destroyed database');
	if (db.isOpen() || existsSync(path)) {
		throw new Error('Expected destroy to close every handle and remove the database');
	}
} else if (mode === 'callback-close') {
	RocksDatabase.open(path).close();
	const readOnly = RocksDatabase.open(path, { readOnly: true });
	let flushed = false;
	(readOnly as any).store.db.flush(
		() => {
			readOnly.close();
			flushed = true;
		},
		(error: unknown) => {
			throw error;
		},
		{}
	);
	if (!flushed || (readOnly as any).store.isOpen())
		throw new Error('Expected flush to resolve and close');

	const db = RocksDatabase.open(path);
	const column = RocksDatabase.open(path, { name: 'column' });
	column.putSync('key', 'value');
	let dropped = false;
	(column as any).store.db.drop(
		() => {
			column.close();
			dropped = true;
		},
		(error: unknown) => {
			throw error;
		}
	);
	if (!dropped || (column as any).store.isOpen())
		throw new Error('Expected drop to resolve and close');
	db.close();
} else {
	throw new Error(`Unknown mode ${mode}`);
}
