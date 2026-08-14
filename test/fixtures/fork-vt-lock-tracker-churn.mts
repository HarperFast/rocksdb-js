/**
 * Isolated repro for the VT LockTracker holder/refcount double-release and
 * TransactionHandle::txn UAF/double-free under worker-env churn
 * (HarperFast/rocksdb-js#741).
 *
 * N worker_threads share one DB path with the Verification Table
 * materialized, all writing through coordinatedRetry transactions on a single
 * hot key (see test/workers/vt-lock-tracker-churn-worker.mts for the per-role
 * shape). One worker is recycled periodically via a GRACEFUL stop (finish
 * current iteration, close its own DB handle, exit) while the other workers
 * are still mid-commit through the same process-global DBDescriptor
 * (AGENTS.md note 6).
 *
 * Before the #741 fix this reliably produced, within roughly a minute:
 *   - a double-release on a shared LockTracker (releaseWriteIntent reaching
 *     "last holder" twice for the same tracker address -- holders/refcount
 *     corruption), and/or
 *   - a UAF/double-free on TransactionHandle::txn via
 *     ~OptimisticTransaction/~TransactionBaseImpl (glibc "double free or
 *     corruption (!prev)" / SIGSEGV).
 *
 * Exit 0 = survived; a crash exits via signal / non-zero. The
 * ROCKSDB_JS_TXN_CLOSE_DELAY_MS seam widens the close()-vs-commit window so
 * the race reproduces within the bounded duration used here instead of
 * needing the full 1-2 minutes the original investigation ran.
 */
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync, rmSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];
const durationMs = Number(process.argv[3] ?? 15_000);
const workerCount = Number(process.argv[4] ?? 4);
const recycleEveryMs = Number(process.argv[5] ?? 4_000);

if (!dbPath) {
	console.error(
		'Usage: fork-vt-lock-tracker-churn.mts <dbPath> [durationMs] [workers] [recycleEveryMs]'
	);
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

let nextId = 0;
const live = new Map<number, Worker>();

function spawnWorker(): Promise<number> {
	const id = nextId++;
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/vt-lock-tracker-churn-worker.mts'),
			{ eval: true, workerData: { dbPath, id } }
		);
		worker.once('online', () => {
			live.set(id, worker);
			resolve(id);
		});
		worker.once('error', reject);
	});
}

function stopWorkerGracefully(id: number): Promise<void> {
	const worker = live.get(id);
	if (!worker) return Promise.resolve();
	// Wait for the worker's own `exit` (it calls process.exit() itself after
	// db.close()) rather than also calling worker.terminate() afterward --
	// see the module doc for why terminate()ing an already-exiting worker is
	// itself a race, not a no-op.
	return new Promise<void>((resolve) => {
		worker.once('exit', () => resolve());
		worker.postMessage('stop');
	});
}

async function run(): Promise<void> {
	for (let i = 0; i < workerCount; i++) {
		await spawnWorker();
	}

	const deadline = Date.now() + durationMs;
	while (Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, recycleEveryMs));
		if (Date.now() >= deadline) break;
		const ids = [...live.keys()];
		if (ids.length === 0) continue;
		// Recycle a middle worker (not the newest) while the rest keep churning --
		// the shape a recycled HTTP worker leaves in production.
		const victim = ids[Math.floor(ids.length / 2)];
		await stopWorkerGracefully(victim);
		live.delete(victim);
		await spawnWorker();
	}

	// Final sweep: gracefully stop everything still running.
	await Promise.all([...live.keys()].map((id) => stopWorkerGracefully(id)));
}

try {
	await run();
	console.log('SUCCESS');
	try {
		rmSync(dbPath, { recursive: true, force: true });
	} catch {
		// best-effort cleanup
	}
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
