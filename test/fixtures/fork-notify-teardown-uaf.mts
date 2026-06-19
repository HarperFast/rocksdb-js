/**
 * Isolated repro for the per-database event-emitter notify() vs. worker-env
 * teardown use-after-free (HarperFast/harper#1370).
 *
 * Scenario:
 * 1. Main pins the shared DBDescriptor by opening the path for the whole run.
 * 2. A long-lived committer worker (same path) streams async transaction
 *    commits; each fires descriptor->notify('committed') on its threadpool.
 * 3. Listener workers (same path) are spawned and torn down in a tight loop.
 *    Each registers a per-db 'committed' listener — a threadsafe function bound
 *    to that worker's env — and does nothing else.
 * 4. Terminating a listener worker tears its env down WHILE the committer's
 *    notify is iterating that listener's tsfn.
 *
 * Before the fix, notify snapshotted + acquired tsfns under the emitter mutex
 * but called them after unlocking; env teardown frees the tsfn out from under
 * that post-unlock call (the acquire is a tsfn-level count env teardown doesn't
 * honor) -> SIGABRT in napi_call_threadsafe_function. After the fix, notify
 * holds the mutex across the call, so a dying worker's cleanup hook can't
 * complete (and Node can't free the tsfn) until the call returns.
 *
 * Listener workers hold no transactions and main never closes mid-commit, so
 * this isolates the notify path from the unrelated transaction-close double
 * free. Exit 0 = survived; a crash exits via signal / non-zero.
 */
import { RocksDatabase } from '../../src/index.js';
import { createWorkerBootstrapScript } from '../lib/util.js';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-notify-teardown-uaf.mts <dbPath>');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

// More rounds = more teardown-vs-notify windows per process; the race only
// surfaces on a fraction of attempts. Bun's worker spawn/teardown is roughly an
// order of magnitude slower than Node's (each round spawns + terminates a
// worker), so a full 60 rounds blows the test timeout on Bun + macOS/Windows.
// The race itself is a runtime-agnostic native C++ bug; Node/Deno carry the
// primary detection, so Bun runs a reduced round count as secondary coverage.
const ROUNDS = process.versions.bun ? 20 : 60;

function spawnWorker(role: 'committer' | 'listener'): Promise<Worker> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/notify-teardown-worker.mts'),
			{ eval: true, workerData: { path: dbPath, role } }
		);
		worker.once('message', (event: { ready?: boolean }) => {
			if (event.ready) {
				resolve(worker);
			}
		});
		worker.once('error', reject);
	});
}

async function run(): Promise<void> {
	// Pin the shared descriptor for the whole run, and keep a main-env listener
	// so the committer's notify always takes its locked path.
	const db = RocksDatabase.open(dbPath);
	db.addListener('committed', () => {});

	const committer = await spawnWorker('committer');

	for (let round = 0; round < ROUNDS; round++) {
		const listener = await spawnWorker('listener');
		// Let the committer's notify('committed') start landing on this listener's
		// tsfn, then tear its env down mid-notify.
		await delay(2);
		await listener.terminate();
	}

	// Stop the committer between commits (no in-flight commit during teardown),
	// then terminate it.
	await new Promise<void>((resolve) => {
		committer.once('message', (event: { stopped?: boolean }) => {
			if (event.stopped) {
				resolve();
			}
		});
		committer.postMessage({ stop: true });
	});
	await committer.terminate();
}

try {
	await run();
	console.log('SUCCESS');
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
