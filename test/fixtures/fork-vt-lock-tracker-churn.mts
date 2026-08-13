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
 * Deliberately GRACEFUL-only, and deliberately waits for the worker's own
 * `exit` event rather than also calling worker.terminate() after it --
 * matching the proven repro-crossthread.mjs shape; terminate()ing a worker
 * that is already mid-voluntary-exit is a redundant double-teardown, not a
 * no-op.
 *
 * The default recycle interval and the worker's holder-role hold duration
 * (test/workers/vt-lock-tracker-churn-worker.mts) deliberately match
 * repro-crossthread.mjs's proven-safe cadence rather than a more aggressive
 * one -- driving the SAME mechanism much harder (worker recycling every
 * ~1.5s instead of ~4s+, VT-lock holds of ~20ms instead of ~200ms) surfaces
 * a DIFFERENT, more severe crash at a much higher rate. But even at THIS
 * tuned cadence it is not reliably clean: a 6-run verification batch hit it
 * once (1/6, exit 139) after 5 consecutive clean runs. That crash is
 * `malloc_printerr("corrupted double-linked list")` inside
 * `node::BaseObjectList::Cleanup()`, reached from
 * `node::Environment::RunCleanup()` during ordinary worker teardown, with
 * no rocksdb-js frames in the corrupting stack. It was NOT root-caused
 * within the time available for this fix, but a targeted comparison points
 * at the VT/coordinatedRetry mechanism specifically rather than generic
 * worker-teardown churn: test/fixtures/fork-commit-teardown.mts drives a
 * plain (non-VT) shared DBDescriptor through 40 rounds of worker
 * spawn+abrupt-terminate() with heavy in-flight async commits and a
 * *shorter* 3ms per-round delay -- objectively more aggressive teardown
 * churn than this fixture's ~2-3 recycles per run -- and reproduced zero
 * crashes across repeated local runs. The differentiator is this fixture's
 * use of coordinatedRetry transactions against a materialized VT (LockTracker
 * park/wake TSFNs), which fork-commit-teardown.mts doesn't exercise at all.
 * That implicates the coordinatedRetry park/wake path as a plausible
 * contributor -- notably the same general area (LockTracker wake-callback
 * lifecycle) as a separate, concurrent investigation into
 * `LockTracker::wakeCallbacks` accumulation -- but this is circumstantial,
 * not a confirmed root cause. See the dispatch findings for #741 for the
 * full bisection data and a saved gdb backtrace; this needs its own
 * dedicated investigation, so the test below is skipped by default rather
 * than shipped as a flake risk.
 *
 * Exit 0 = survived; a crash exits via signal / non-zero. The
 * ROCKSDB_JS_TXN_CLOSE_DELAY_MS seam (shared with
 * fork-close-commit-uaf.mts) widens the close()-vs-commit window so the
 * race reproduces within the bounded duration used here instead of needing
 * the full 1-2 minutes the original investigation ran.
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
