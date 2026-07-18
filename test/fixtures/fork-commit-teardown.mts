/**
 * Isolated repro for async-commit completion vs. worker-env teardown.
 *
 * The async commit path dispatches each commit to the database's dedicated
 * commit thread (owned by the shared DBDescriptor) and marshals the completion
 * back to the originating env via a persistent threadsafe function. This
 * fixture terminates a worker env while its commits are still queued/executing
 * on that shared thread:
 *
 * 1. Main pins the shared DBDescriptor by opening the path for the whole run,
 *    so the commit thread outlives each worker.
 * 2. A committer worker (same path) streams async commits without awaiting
 *    them, keeping many in-flight.
 * 3. The parent terminates the worker mid-stream. Its commit tsfn is released
 *    by the DBHandle finalizer, but the commit thread may still call it to
 *    complete the worker's already-queued commits.
 *
 * Crash-safety comes from the persistent per-env tsfn on
 * `DBDescriptor::commitCompletions`: the module env-cleanup hook
 * (`DBRegistry::ReleaseCommitCompletionsByEnv`) releases the worker's entry
 * under `commitMutex` before Node frees the env's tsfns, and the commit
 * thread only calls a completion under that same mutex — so it either
 * delivers safely or observes the entry gone and drops it. A per-commit
 * `napi_acquire_threadsafe_function` would NOT close this window (env
 * teardown does not honor the tsfn-level acquire count).
 * Exit 0 = survived; a crash exits via signal / non-zero.
 *
 * Set ROCKSDB_JS_COMMIT_DELAY_MS (see the test) to widen the window so the
 * race reproduces deterministically.
 */
import { RocksDatabase } from '../../src/index.js';
import { createWorkerBootstrapScript } from '../lib/util.js';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-commit-teardown.mts <dbPath>');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

// Worker spawn/teardown dominates the wall time and each round queues
// ROCKSDB_JS_COMMIT_DELAY_MS x IN_FLIGHT of delayed commit work on the shared
// commit thread, so round count is scaled down where spawns are slow (Bun
// everywhere; macOS/Windows CI runners). The race is a runtime-agnostic
// native lifecycle bug and Linux Node carries the primary detection rate.
const ROUNDS = process.versions.bun
	? 10
	: process.platform === 'darwin' || process.platform === 'win32'
		? 16
		: 40;

function spawnCommitter(): Promise<Worker> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/commit-teardown-worker.mts'),
			{ eval: true, workerData: { path: dbPath } }
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
	// Pin the shared descriptor (and its commit thread) for the whole run.
	const db = RocksDatabase.open(dbPath);

	for (let round = 0; round < ROUNDS; round++) {
		const committer = await spawnCommitter();
		// Let commits start landing on the commit thread, then tear the env down
		// with commits still queued/in-flight.
		await delay(3);
		await committer.terminate();
	}

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
