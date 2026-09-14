/**
 * Isolated repro for rocksdb-js#848: an unlock callback queued by a worker env
 * that is later terminated must not be called when another env unlocks.
 *
 * Scenario:
 * 1. Main opens the path and takes `tryLock(key)`.
 * 2. A worker opens the same path and calls `tryLock(key, callback)`; the lock
 *    is held, so the callback -- a threadsafe function of the worker's env -- is
 *    queued on the shared LockHandle.
 * 3. Main terminates the worker. Node tears the worker env down and frees its
 *    tsfns.
 * 4. Main calls `unlock(key)`.
 *
 * Before the fix, `lockReleaseByKey` called the freed tsfn: Node 22 aborts the
 * process inside `napi_call_threadsafe_function`; Node 24 returns
 * `napi_closing`. After the fix, the worker env's cleanup hook removes its
 * queued callbacks under the lock mutex, so unlock finds nothing of that env.
 * Exit 0 = survived; a crash exits via signal / non-zero.
 */
import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-lock-teardown-abort.mts <dbPath>');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

// Each round is one full queue -> terminate -> unlock sequence; the failure is
// deterministic on an affected runtime, so a few rounds are only insurance.
const ROUNDS = 3;

function spawnWaiter(key: string): Promise<Worker> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/lock-teardown-worker.mts'),
			{
				eval: true,
				workerData: { path: dbPath, key },
			}
		);
		worker.once('message', (event: { ready?: boolean; acquired?: boolean }) => {
			if (!event.ready) {
				return;
			}
			if (event.acquired) {
				reject(new Error('the worker acquired a lock the main thread holds'));
				return;
			}
			resolve(worker);
		});
		worker.once('error', reject);
	});
}

async function run(): Promise<void> {
	const db = RocksDatabase.open(dbPath);
	for (let round = 0; round < ROUNDS; round++) {
		const key = `lock-${round}`;
		if (!db.tryLock(key)) {
			throw new Error(`main could not take ${key}`);
		}
		const waiter = await spawnWaiter(key);
		await waiter.terminate();
		// Let the terminated env's cleanup settle before the release that used to
		// call into it; the fix must hold with or without this gap.
		await delay(round === 0 ? 0 : 20);
		db.unlock(key);
	}
	// A surviving waiter must still be woken: prove the fix did not drop live callbacks.
	if (!db.tryLock('live')) {
		throw new Error('main could not take the live lock');
	}
	const live = await spawnWaiter('live');
	const fired = new Promise<void>((resolve) => {
		live.once('message', (event: { fired?: boolean }) => {
			if (event.fired) {
				resolve();
			}
		});
	});
	db.unlock('live');
	await fired;
	await live.terminate();
	db.close();
}

try {
	await run();
	console.log('SUCCESS');
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
