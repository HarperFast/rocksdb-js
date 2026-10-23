/**
 * Child-process scenarios for pending lock and park wakes, isolated because a
 * failure is a process abort or a process that never exits, and the commit
 * thread mode is fixed per process.
 *
 * - `withLock` / `tryLock` / `park`: a worker waits on a wake from this thread
 *   while nothing else keeps its loop alive, and must receive it. `park` must
 *   resume and commit its coordinated-retry transaction.
 * - `same-thread`: an uncontended `await db.withLock()` with nothing else alive.
 * - `worker-exit`: a worker exits with a lock call dispatched to it; the process
 *   must survive and the lock must pass to the next waiter.
 * - `worker-active-exit`: terminate a worker after its async lock callback starts;
 *   a main-owned lock must pass to the next live waiter.
 *
 * Prints SUCCESS and exits 0 on success.
 */
import { RocksDatabase } from '../../src/index.ts';
import { lockWakeCallbackCount, steadyClockNow } from '../../src/load-binding.ts';
import { Transaction } from '../../src/transaction.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const [mode, dbPath] = process.argv.slice(2);

if (!dbPath) {
	console.error(
		'Usage: fork-pending-wake.mts <withLock|tryLock|park|same-thread|worker-exit|worker-active-exit> <dbPath>'
	);
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

/**
 * Runs `pending-wake-worker.mts` and calls `release` long enough after the
 * worker is waiting for an unref'd loop to have drained. Returns the event order.
 */
async function runWaiter(
	key: string,
	release: () => unknown,
	waiting: () => Promise<void> = async () => {}
): Promise<string[]> {
	const events: string[] = [];
	let released: Promise<unknown> = Promise.resolve();
	const worker = new Worker(createWorkerBootstrapScript('./test/workers/pending-wake-worker.mts'), {
		eval: true,
		workerData: { path: dbPath, mode, key },
	});
	await new Promise<void>((resolve, reject) => {
		worker.on('message', (event: { waiting?: boolean; woken?: string }) => {
			if (event.waiting) {
				released = waiting()
					.then(() => delay(250))
					.then(() => {
						events.push('released');
						return release();
					});
			} else if (event.woken) {
				events.push(`woken:${event.woken}`);
			}
		});
		worker.once('error', reject);
		worker.once('exit', (code) => {
			events.push(`exit:${code}`);
			resolve();
		});
	});
	await released;
	return events;
}

/**
 * The worker reports `waiting` from inside its transaction body, before the
 * commit parks; releasing before the park registers would let the retry run
 * without ever parking.
 */
async function parkRegistered(): Promise<void> {
	const deadline = steadyClockNow() + 10_000;
	while (lockWakeCallbackCount() === 0) {
		if (steadyClockNow() > deadline) {
			throw new Error('the worker commit never parked');
		}
		await delay(5);
	}
}

function expectEvents(actual: string[], expected: string[]): void {
	if (JSON.stringify(actual) !== JSON.stringify(expected)) {
		throw new Error(`expected events ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
	}
}

async function run(): Promise<void> {
	if (mode === 'withLock') {
		const db = RocksDatabase.open(dbPath);
		let releaseHolder!: () => void;
		const holding = db.withLock(
			'key',
			() => new Promise<void>((resolve) => (releaseHolder = resolve))
		);
		const events = await runWaiter('key', () => {
			releaseHolder();
			return holding;
		});
		expectEvents(events, ['released', 'woken:ok', 'exit:0']);
		db.close();
	} else if (mode === 'tryLock') {
		const db = RocksDatabase.open(dbPath);
		if (!db.tryLock('key')) {
			throw new Error('main could not take the lock');
		}
		const events = await runWaiter('key', () => db.unlock('key'));
		expectEvents(events, ['released', 'woken:ok', 'exit:0']);
		db.close();
	} else if (mode === 'park') {
		const db = RocksDatabase.open(dbPath, { verificationTable: true });
		await db.put('hot', 'v0');
		db.populateVersion('hot', 1.5e12); // materialize the VT slot so write intents engage
		const holder = new Transaction(db.store, { coordinatedRetry: true });
		await holder.put('hot', 'held');
		const events = await runWaiter('hot', () => holder.abort(), parkRegistered);
		// The worker reports the value its retried transaction committed.
		expectEvents(events, ['released', 'woken:attempt-2', 'exit:0']);
		if ((await db.get('hot')) !== 'attempt-2') {
			throw new Error(`expected the retried commit to land, got ${await db.get('hot')}`);
		}
		db.close();
	} else if (mode === 'same-thread') {
		const db = RocksDatabase.open(dbPath);
		let ran = false;
		await db.withLock('key', () => {
			ran = true;
		});
		if (!ran) {
			throw new Error('withLock resolved without running its callback');
		}
		db.close();
	} else if (mode === 'worker-active-exit') {
		const db = RocksDatabase.open(dbPath);
		let releaseHolder!: () => void;
		const holding = db.withLock(
			'key',
			() => new Promise<void>((resolve) => (releaseHolder = resolve))
		);
		const script = `
			const { RocksDatabase } = await import(${JSON.stringify(new URL('../../src/index.ts', import.meta.url).href)});
			const { parentPort, workerData } = await import('node:worker_threads');
			parentPort.on('message', () => {});
			const db = RocksDatabase.open(workerData.path);
			db.withLock('key', () => {
				parentPort.postMessage('started');
				return new Promise(() => {});
			});
			parentPort.postMessage('queued');
		`;
		const worker = new Worker(`(async () => {${script}})()`, {
			eval: true,
			workerData: { path: dbPath },
		});
		const started = new Promise<void>((resolve, reject) => {
			worker.on('message', (message) => {
				if (message === 'started') resolve();
			});
			worker.once('error', reject);
		});
		await new Promise((resolve) => worker.once('message', resolve));
		let ran = false;
		const next = db.withLock('key', () => {
			ran = true;
		});
		releaseHolder();
		await holding;
		await started;
		await worker.terminate();
		await next;
		if (!ran || db.hasLock('key')) {
			throw new Error('the active worker call stranded the next waiter');
		}
		db.close();
	} else if (mode === 'worker-exit') {
		// This thread's handle keeps the descriptor and the lock alive, so the
		// worker's teardown cannot erase the lock: only completing its
		// dispatched call passes the lock on.
		const db = RocksDatabase.open(dbPath);
		let releaseHolder!: () => void;
		const holding = db.withLock(
			'key',
			() => new Promise<void>((resolve) => (releaseHolder = resolve))
		);
		const gate = new Int32Array(new SharedArrayBuffer(4));
		const script = `
			const { RocksDatabase } = await import(${JSON.stringify(new URL('../../src/index.ts', import.meta.url).href)});
			const { parentPort, workerData } = await import('node:worker_threads');
			const db = RocksDatabase.open(workerData.path);
			db.withLock('key', () => {});
			parentPort.postMessage('queued');
			// Never return to the loop: the call dispatched to this env must
			// still be queued when it exits.
			Atomics.wait(workerData.gate, 0, 0);
			process.exit(0);
		`;
		const worker = new Worker(`(async () => {${script}})()`, {
			eval: true,
			workerData: { path: dbPath, gate },
		});
		const exited = new Promise<number>((resolve, reject) => {
			worker.once('exit', resolve);
			worker.once('error', reject);
		});
		await new Promise((resolve) => worker.once('message', resolve));
		releaseHolder();
		await holding; // completion has dispatched the worker's call
		Atomics.store(gate, 0, 1);
		Atomics.notify(gate, 0);
		const code = await exited;
		if (code !== 0) {
			throw new Error(`worker exited with ${code}`);
		}
		let ran = false;
		await db.withLock('key', () => {
			ran = true;
		});
		if (!ran || db.hasLock('key')) {
			throw new Error('the lock was not passed on after the worker exited');
		}
		db.close();
	} else {
		throw new Error(`Unknown mode: ${mode}`);
	}
}

try {
	await run();
	console.log('SUCCESS');
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
