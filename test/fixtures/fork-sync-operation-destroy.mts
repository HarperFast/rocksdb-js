import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A guarded synchronous call registers on its own DBHandle, not on the shared
// descriptor, so a foreign destroy() must find every handle with a call in
// flight among the descriptor's closables and wait for each. A call is held
// open by an options getter, which the native getCount() reads after its
// admission check.
//
//   destroy:            the main thread and a second worker are each inside a
//                       call while a third worker destroys the database.
//   reentrant:          close() and destroy() from such a getter are refused,
//                       and the database stays usable.
//   callback-close:     calls that settle through a JavaScript callback end
//                       their admission first, so closing from it succeeds.

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

if (mode === 'destroy') {
	const db = RocksDatabase.open(path);
	db.putSync('key', 'value');
	const native = (db as any).store.db;
	const state = new Int32Array(new SharedArrayBuffer(12));
	const holder = startWorker(state);
	const destroyer = startWorker(state);
	await Promise.all([holder.ready, destroyer.ready]);

	holder.worker.postMessage('hold');
	waitUntil(state, HOLDER, 0, 'the second handle to enter its call');

	let observed = '';
	const callError = holdInGetCount(native, () => {
		destroyer.worker.postMessage('destroy');
		waitUntil(state, DESTROY, 0, 'destroy to start');
		const deadline = Date.now() + 5_000;
		while (native.opened) {
			if (Date.now() > deadline) throw new Error('destroy never published closing');
			Atomics.wait(state, DESTROY, 1, 5);
		}
		observed =
			Atomics.wait(state, DESTROY, 1, 300) === 'timed-out'
				? 'waited'
				: 'destroy returned while the main thread was inside a synchronous call';
	});
	if (observed !== 'waited') throw new Error(observed || String(callError));
	if (!String(callError).includes('Database is closing')) {
		throw new Error(`Expected the scan to observe the close, got ${String(callError)}`);
	}

	if (Atomics.wait(state, DESTROY, 1, 300) !== 'timed-out') {
		throw new Error('destroy returned while the second handle was inside a synchronous call');
	}
	Atomics.store(state, RELEASE, 1);
	Atomics.notify(state, RELEASE);

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
