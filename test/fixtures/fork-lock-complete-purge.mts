import { getRegistryStatus, RocksDatabase } from '../../src/index.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

// Run with ROCKSDB_JS_LOCK_COMPLETE_DELAY_MS so a withLock() completion holds its descriptor
// reference while a worker closes the last handle: that close skips its purge, and the completion
// must retry it once it lets go.
const WAIT_MS = 10_000;
const MAIN_CLOSED = 0;
const MAIN_CHECKED = 1;
const BASELINE = 2;

const entryOf = (path: string) => getRegistryStatus().find((entry) => entry.path === path);

function waitFor(signal: Int32Array, index: number, label: string): void {
	if (Atomics.wait(signal, index, 0, WAIT_MS) === 'timed-out') {
		throw new Error(`Timed out waiting for ${label}`);
	}
}

if (!isMainThread) {
	const { path, signal } = workerData as { path: string; signal: Int32Array };
	const db = RocksDatabase.open(path);
	parentPort!.postMessage('opened');
	waitFor(signal, MAIN_CLOSED, 'the main thread to close its handle');
	// The completion pins the descriptor only after the callback returns, so close once it has.
	const baseline = Atomics.load(signal, BASELINE);
	const deadline = Date.now() + WAIT_MS;
	let refCount = 0;
	while ((refCount = entryOf(path)?.refCount ?? 0) <= baseline) {
		if (Date.now() > deadline) {
			throw new Error(
				`The withLock() completion never pinned the descriptor (refCount ${refCount}, baseline ${baseline})`
			);
		}
	}
	db.close();
	parentPort!.postMessage(entryOf(path) ? 'overlapped' : 'purged');
	// This env's cleanup hook would purge a leaked entry, so stay alive until the main thread checks.
	waitFor(signal, MAIN_CHECKED, 'the main thread to check the registry');
} else {
	for (const shape of ['sync', 'async'] as const) {
		const path = join(process.argv[2], shape);
		mkdirSync(path, { recursive: true });
		const db = RocksDatabase.open(path);
		const signal = new Int32Array(new SharedArrayBuffer(12));
		const worker = new Worker(new URL(import.meta.url), { workerData: { path, signal } });
		const failed = new Promise<never>((_resolve, reject) => worker.once('error', reject));
		const exited = new Promise<number>((resolve) => worker.once('exit', resolve));
		const nextMessage = () =>
			Promise.race([new Promise<string>((resolve) => worker.once('message', resolve)), failed]);

		await nextMessage();
		const closed = nextMessage();
		const releaseToWorker = () => {
			db.close();
			Atomics.store(signal, BASELINE, entryOf(path)!.refCount);
			Atomics.store(signal, MAIN_CLOSED, 1);
			Atomics.notify(signal, MAIN_CLOSED);
		};
		await db.withLock(
			'key',
			shape === 'sync'
				? releaseToWorker
				: async () => {
						releaseToWorker();
					}
		);
		const outcome = await closed;
		const leaked = entryOf(path);
		Atomics.store(signal, MAIN_CHECKED, 1);
		Atomics.notify(signal, MAIN_CHECKED);
		const code = await Promise.race([exited, failed]);
		if (code !== 0) throw new Error(`${shape}: worker exited with code ${code}`);
		if (outcome !== 'overlapped') {
			throw new Error(`${shape}: the worker's close did not overlap the completion (${outcome})`);
		}
		if (leaked) {
			throw new Error(
				`${shape}: descriptor stayed registered after its last handle closed: ${JSON.stringify(leaked)}`
			);
		}
	}
}
