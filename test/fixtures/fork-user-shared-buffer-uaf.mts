/**
 * Isolated repro for cross-handle user shared buffer lifetime vs. DBDescriptor::close().
 *
 * Scenario:
 * 1. Main + worker each open the same path/CF (shared DBDescriptor).
 * 2. Both obtain the same user-shared ArrayBuffer mapping.
 * 3. Main closes, then worker closes -> last handle triggers descriptor->close()
 *    (columns cleared while external ArrayBuffers may still be live on main).
 * 4. Main keeps reading/writing the retained ArrayBuffer (and optionally hammers
 *    getRange on a stale handle) to surface UAF / SIGSEGV.
 *
 * Exit 0 = survived (expected once the binding pins buffer lifetime correctly).
 */
import { RocksDatabase } from '../../src/index.js';
import { createWorkerBootstrapScript } from '../lib/util.js';
import { mkdirSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];
const mode = process.argv[3] ?? 'buffer';

if (!dbPath) {
	console.error(
		'Usage: fork-user-shared-buffer-uaf.mts <dbPath> [buffer|buffer-race|stale-iterator|schema-sync]'
	);
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

const KEY = 'uaf-repro-key';
const CF_NAME = 'uaf-repro-cf';

function hammerBuffer(buffer: ArrayBuffer, iterations: number): void {
	const view = new DataView(buffer);
	const magic = 0xc0ffee1234567890n;
	view.setBigUint64(0, magic);

	for (let i = 0; i < iterations; i++) {
		view.setBigUint64(0, BigInt(i));
		const read = view.getBigUint64(0);
		if (read !== BigInt(i)) {
			console.error(`Buffer read mismatch at i=${i}: got ${read}`);
			process.exit(1);
		}
	}
}

async function closeAllHandlesViaWorker(): Promise<ArrayBuffer> {
	const db = RocksDatabase.open(dbPath, { name: CF_NAME });
	const buffer = db.getUserSharedBuffer(KEY, new ArrayBuffer(8));

	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/user-shared-buffer-uaf-close-worker.mts'),
		{
			eval: true,
			workerData: { path: dbPath, name: CF_NAME, key: KEY },
		}
	);

	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { started?: boolean }) => {
			if (event.started) {
				resolve();
			}
		});
		worker.once('error', reject);
	});

	// Drop main handle first, then worker — worker close is often the last ref.
	db.close();

	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { closed?: boolean }) => {
			if (event.closed) {
				resolve();
			}
		});
		worker.once('error', reject);
		worker.postMessage({ close: true });
	});

	await worker.terminate();

	return buffer;
}

async function runBufferMode(): Promise<void> {
	const buffer = await closeAllHandlesViaWorker();
	console.log('All handles closed; hammering retained ArrayBuffer...');
	hammerBuffer(buffer, 500_000);
	console.log('Buffer hammer completed without mismatch');
}

/**
 * Hammer the mapping while main + worker handles are being closed (narrower race
 * than post-close hammering).
 */
async function runBufferRaceMode(): Promise<void> {
	const db = RocksDatabase.open(dbPath, { name: CF_NAME });
	const buffer = db.getUserSharedBuffer(KEY, new ArrayBuffer(8));
	const view = new DataView(buffer);

	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/user-shared-buffer-uaf-close-worker.mts'),
		{
			eval: true,
			workerData: { path: dbPath, name: CF_NAME, key: KEY },
		}
	);

	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { started?: boolean }) => {
			if (event.started) {
				resolve();
			}
		});
		worker.once('error', reject);
	});

	let stop = false;
	const spin = () => {
		if (stop) {
			return;
		}
		for (let i = 0; i < 10_000; i++) {
			view.setBigUint64(0, BigInt(i));
			view.getBigUint64(0);
		}
		setImmediate(spin);
	};
	setImmediate(spin);

	// Close main + worker without waiting for the spin loop to finish.
	db.close();
	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { closed?: boolean }) => {
			if (event.closed) {
				resolve();
			}
		});
		worker.once('error', reject);
		worker.postMessage({ close: true });
	});
	await worker.terminate();
	stop = true;

	console.log('Close race finished; post-close hammer...');
	hammerBuffer(buffer, 500_000);
	console.log('Buffer race + post-close hammer completed');
}

async function runStaleIteratorMode(): Promise<void> {
	const db = RocksDatabase.open(dbPath, { name: CF_NAME });
	db.putSync('k', 'v');

	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/user-shared-buffer-uaf-close-worker.mts'),
		{
			eval: true,
			workerData: { path: dbPath, name: CF_NAME, key: KEY },
		}
	);

	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { started?: boolean }) => {
			if (event.started) {
				resolve();
			}
		});
		worker.once('error', reject);
	});

	const range = db.getRange();
	const iterator = range[Symbol.iterator]();

	// Prime the native iterator.
	iterator.next();

	// Schedule descriptor teardown on the worker while this thread keeps calling
	// into native Next() — closable->close() vs. DBIterator::Next().
	setImmediate(() => {
		db.close();
		worker.postMessage({ close: true });
	});

	let nativeErrors = 0;
	for (let i = 0; i < 500_000; i++) {
		try {
			iterator.next();
		} catch {
			nativeErrors++;
			if (nativeErrors > 3) {
				break;
			}
		}
	}

	await new Promise<void>((resolve, reject) => {
		worker.once('message', (event: { closed?: boolean }) => {
			if (event.closed) {
				resolve();
			}
		});
		worker.once('error', reject);
	});
	await worker.terminate();

	console.log(`Stale iterator loop finished (caught errors=${nativeErrors})`);
}

/**
 * Harper-like schema sync: one thread repeatedly open/close the same CF while
 * others retain a cross-thread ArrayBuffer mapping and hammer it.
 */
async function runSchemaSyncMode(): Promise<void> {
	const db = RocksDatabase.open(dbPath, { name: CF_NAME });
	const buffer = db.getUserSharedBuffer(KEY, new ArrayBuffer(8));
	const view = new DataView(buffer);
	view.setBigUint64(0, 1n);

	const holder = new Worker(
		createWorkerBootstrapScript('./test/workers/user-shared-buffer-uaf-close-worker.mts'),
		{
			eval: true,
			workerData: { path: dbPath, name: CF_NAME, key: KEY },
		}
	);

	const syncWorker = new Worker(
		createWorkerBootstrapScript('./test/workers/user-shared-buffer-uaf-schema-sync-worker.mts'),
		{
			eval: true,
			workerData: { path: dbPath, name: CF_NAME, key: KEY, cycles: 200 },
		}
	);

	await Promise.all([
		new Promise<void>((resolve, reject) => {
			holder.once('message', (event: { started?: boolean }) => {
				if (event.started) {
					resolve();
				}
			});
			holder.once('error', reject);
		}),
		new Promise<void>((resolve, reject) => {
			syncWorker.once('message', (event: { started?: boolean }) => {
				if (event.started) {
					resolve();
				}
			});
			syncWorker.once('error', reject);
		}),
	]);

	let stop = false;
	const spin = () => {
		if (stop) {
			return;
		}
		for (let i = 0; i < 5_000; i++) {
			Atomics.add(new BigInt64Array(buffer), 0, 1n);
		}
		setImmediate(spin);
	};
	setImmediate(spin);

	syncWorker.postMessage({ run: true });
	await new Promise<void>((resolve, reject) => {
		syncWorker.once('message', (event: { done?: boolean }) => {
			if (event.done) {
				resolve();
			}
		});
		syncWorker.once('error', reject);
	});

	stop = true;
	// Forceful terminate (no explicit db.close() in either worker) is
	// intentional: this exercises the case where worker DBHandles are reaped
	// by N-API env teardown rather than orderly close, mirroring abnormal
	// worker exits seen in production.
	await syncWorker.terminate();
	await holder.terminate();

	console.log('Schema-sync cycles complete; post-close hammer...');
	db.close();
	hammerBuffer(buffer, 500_000);
	console.log('Schema-sync hammer completed');
}

try {
	if (mode === 'stale-iterator') {
		await runStaleIteratorMode();
	} else if (mode === 'buffer-race') {
		await runBufferRaceMode();
	} else if (mode === 'buffer') {
		await runBufferMode();
	} else if (mode === 'schema-sync') {
		await runSchemaSyncMode();
	} else {
		console.error(`Unknown mode: ${mode}`);
		process.exit(1);
	}
	console.log('SUCCESS');
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
