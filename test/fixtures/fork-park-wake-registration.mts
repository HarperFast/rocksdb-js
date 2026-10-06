/**
 * A coordinated-retry park registers a wake callback on the conflicting holder's VT lock. Every way
 * a park can end must remove that registration, or a holder that never releases accumulates one per
 * re-park. `lockWakeCallbackCount()` is process-wide, which is why this runs in its own process.
 *
 * Scenarios (argv[2]):
 * - `timeout`: run with a short ROCKSDB_JS_PARK_TIMEOUT_MS; repeated parks behind one held lock
 *   each time out and leave nothing registered while the holder still holds.
 * - `wake`: the default timeout; releasing the holder wakes a registered park long before it.
 * - `worker-exit`: a worker parked behind the main thread's holder is terminated; its env cleanup
 *   removes the registration while the holder still holds.
 * - `foreign-close`: a one-slot verification table puts a database's park on another database's
 *   tracker; closing the waiting database removes the registration while the holder still holds.
 */
import { RocksDatabase } from '../../src/index.ts';
import { lockWakeCallbackCount } from '../../src/load-binding.ts';
import { RETRY_NOW } from '../../src/transaction.ts';
import { holdLock, parkBehind } from '../lib/park.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const [scenario, dbPath] = process.argv.slice(2);

if (!scenario || !dbPath) {
	console.error('Usage: fork-park-wake-registration.mts <scenario> <dbPath>');
	process.exit(1);
}

const dbOptions = { encoding: false as const, verificationTable: true };
const key = Buffer.from('park-wake-registration');

async function waitForCount(expected: number, withinMs: number, what: string): Promise<void> {
	const start = performance.now();
	while (lockWakeCallbackCount() !== expected) {
		if (performance.now() - start > withinMs) {
			throw new Error(
				`${what}: ${lockWakeCallbackCount()} wake callbacks registered, expected ${expected} within ${withinMs}ms`
			);
		}
		await delay(1);
	}
}

function expectCount(expected: number, what: string): void {
	const count = lockWakeCallbackCount();
	if (count !== expected) {
		throw new Error(`${what}: ${count} wake callbacks registered, expected ${expected}`);
	}
}

// Run with ROCKSDB_JS_PARK_TIMEOUT_MS=250.
async function timeoutScenario(): Promise<void> {
	const db = RocksDatabase.open(dbPath, dbOptions);
	try {
		const holder = holdLock(db, key);
		for (let round = 0; round < 4; round++) {
			const park = await parkBehind(db, key);
			await waitForCount(1, 200, `round ${round}: park registration`);
			const result = await park.commit;
			const elapsed = performance.now() - park.start;
			if (result !== RETRY_NOW || elapsed < 200) {
				throw new Error(
					`round ${round}: expected a timed-out park, got ${String(result)} after ${elapsed}ms`
				);
			}
			expectCount(0, `round ${round}: after the park timed out`);
			park.transaction.abort();
		}
		holder.abort();
		expectCount(0, 'after the holder released');
	} finally {
		db.close();
	}
}

async function wakeScenario(): Promise<void> {
	const db = RocksDatabase.open(dbPath, dbOptions);
	try {
		const holder = holdLock(db, key);
		const park = await parkBehind(db, key);
		await waitForCount(1, 3000, 'park registration');
		const released = performance.now();
		holder.abort();
		const result = await park.commit;
		const elapsed = performance.now() - released;
		// Well under the 5s default timeout, so only the wake can account for it.
		if (result !== RETRY_NOW || elapsed >= 2500) {
			throw new Error(`expected a woken park, got ${String(result)} after ${elapsed}ms`);
		}
		expectCount(0, 'after the wake');
		park.transaction.abort();
	} finally {
		db.close();
	}
}

async function workerExitScenario(): Promise<void> {
	const db = RocksDatabase.open(dbPath, dbOptions);
	try {
		const holder = holdLock(db, key);
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/park-wake-registration-worker.mts'),
			{ eval: true, workerData: { path: dbPath, key: key.toString() } }
		);
		const failed = new Promise<never>((_, reject) => worker.once('error', reject));
		await Promise.race([waitForCount(1, 3000, 'worker park registration'), failed]);
		await worker.terminate();
		// Under the 5s default timeout, so only the worker env's cleanup can account for it.
		await waitForCount(0, 2000, 'after the parked worker was terminated');
		holder.abort();
		expectCount(0, 'after the holder released');
	} finally {
		db.close();
	}
}

async function foreignCloseScenario(): Promise<void> {
	RocksDatabase.config({ verificationTableEntries: 1 });
	mkdirSync(dbPath, { recursive: true });
	const holderDb = RocksDatabase.open(join(dbPath, 'holder'), dbOptions);
	const waiterDb = RocksDatabase.open(join(dbPath, 'waiter'), dbOptions);
	try {
		const holder = holdLock(holderDb, Buffer.from('holder-key'));
		const park = await parkBehind(waiterDb, Buffer.from('waiter-key'));
		await waitForCount(1, 3000, 'park registration on the foreign tracker');
		waiterDb.close();
		expectCount(0, 'after the waiting database closed');
		await Promise.allSettled([park.commit]);
		holder.abort();
		expectCount(0, 'after the holder released');
	} finally {
		if (waiterDb.isOpen()) {
			waiterDb.close();
		}
		holderDb.close();
	}
}

const scenarios: Record<string, () => Promise<void>> = {
	timeout: timeoutScenario,
	wake: wakeScenario,
	'worker-exit': workerExitScenario,
	'foreign-close': foreignCloseScenario,
};

// A parked commit's threadsafe function is unref'd, so nothing else keeps the loop alive.
setInterval(() => {}, 1000);

try {
	const run = scenarios[scenario];
	if (!run) {
		throw new Error(`unknown scenario ${scenario}`);
	}
	await run();
	console.log('ok');
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
