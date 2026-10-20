/**
 * Child-process repros for rocksdb-js#797. Each runs in its own process because
 * the failures are a process abort, or a waiter that exits with work pending
 * (and the commit-thread mode is fixed per process).
 *
 * - `withLock` / `tryLock` / `park`: a worker waits on a wake this thread
 *   delivers after a delay, holding nothing else that keeps its loop alive. With
 *   the wake's threadsafe function unref'd, the worker exited (code 0) before
 *   the wake arrived. `park` is a coordinated-retry transaction parked on this
 *   thread's write intent; it must resume and commit.
 * - `same-thread`: an uncontended `await db.withLock()` with nothing else keeping
 *   the loop alive never ran its callback; env teardown then ran the queued call
 *   and double-freed its completion data (SIGABRT).
 * - `worker-exit`: a worker queues a lock call and exits before its loop runs it.
 *   Teardown runs the call against a dying env: the same double free aborted the
 *   whole process, and the lock must still pass to the next waiter.
 *
 * Prints SUCCESS and exits 0 on success.
 */
import { RocksDatabase } from '../../src/index.ts';
import { Transaction } from '../../src/transaction.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const [mode, dbPath] = process.argv.slice(2);

if (!dbPath) {
	console.error(
		'Usage: fork-pending-wake.mts <withLock|tryLock|park|same-thread|worker-exit> <dbPath>'
	);
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

/**
 * Runs `pending-wake-worker.mts` and calls `release` long enough after it
 * reports waiting for an unref'd loop to have drained. Returns the event order.
 */
async function runWaiter(key: string, release: () => unknown): Promise<string[]> {
	const events: string[] = [];
	let released: Promise<unknown> = Promise.resolve();
	const worker = new Worker(createWorkerBootstrapScript('./test/workers/pending-wake-worker.mts'), {
		eval: true,
		workerData: { path: dbPath, mode, key },
	});
	await new Promise<void>((resolve, reject) => {
		worker.on('message', (event: { waiting?: boolean; woken?: string }) => {
			if (event.waiting) {
				released = delay(250).then(() => {
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
		const events = await runWaiter('hot', () => holder.abort());
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
	} else if (mode === 'worker-exit') {
		const script = `
			const { RocksDatabase } = await import(${JSON.stringify(new URL('../../src/index.ts', import.meta.url).href)});
			const { workerData } = await import('node:worker_threads');
			const db = RocksDatabase.open(workerData);
			db.withLock('key', () => {});
			process.exit(0);
		`;
		for (let round = 0; round < 3; round++) {
			const worker = new Worker(`(async () => {${script}})()`, { eval: true, workerData: dbPath });
			const code = await new Promise<number>((resolve, reject) => {
				worker.once('exit', resolve);
				worker.once('error', reject);
			});
			if (code !== 0) {
				throw new Error(`worker exited with ${code}`);
			}
		}
		// The lock the exited workers queued must not be left held.
		const db = RocksDatabase.open(dbPath);
		await db.withLock('key', () => {});
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
