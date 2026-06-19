/**
 * Isolated repro for the TransactionHandle::close() double-free when
 * DBDescriptor::close() (env M's JS thread) and the async commit's complete
 * callback (env W's JS thread) both call close() concurrently
 * (HarperFast/harper#1370, close-vs-commit variant).
 *
 * Scenario per round:
 * 1. M opens the DB path (descriptor refcount = 1).
 * 2. M signals W "nextRound"; W opens the same path (refcount = 2).
 * 3. W queues an async commit, then immediately closes W's handle (refcount → 1).
 *    T->dbHandle->descriptor is now null; PATH B's transactionRemove is skipped.
 * 4. W signals M "committing"; M closes M's handle (refcount → 0) →
 *    DBDescriptor::close() → T->close() on M's JS thread (PATH A).
 * 5. PATH A calls waitForAsyncWorkCompletion(), unblocking when the execute
 *    handler signals, then sleeps for ROCKSDB_JS_TXN_CLOSE_DELAY_MS ms (seam).
 * 6. During that sleep the commit's complete callback fires on W's JS thread
 *    (PATH B) and also calls T->close().
 *
 * Before the fix, both paths pass the `!this->txn` guard and both execute
 * `delete this->txn` → double-free through ~OptimisticTransaction →
 * ~PointLockTracker → SIGABRT.  After the fix, the `closed` atomic gate lets
 * only the first caller through; the second returns immediately.
 *
 * Round coordination: W only opens the DB after M sends "nextRound", and M
 * only triggers the next round after sending "readyForNext" to W once M's
 * descriptor has been fully closed and the registry entry erased. This prevents
 * W from reopening the DB before M's close commits to the registry, which would
 * trigger the pre-existing DBRegistry race (HarperFast/rocksdb-js PR #664).
 *
 * Exit 0 = survived; a crash exits via signal / non-zero.
 */
import { RocksDatabase } from '../../src/index.js';
import { createWorkerBootstrapScript } from '../lib/util.js';
import { mkdirSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-close-commit-uaf.mts <dbPath>');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

// More rounds = more PATH-A/PATH-B overlap windows.
const ROUNDS = 60;

function spawnWorker(): Promise<Worker> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/txn-close-commit-worker.mts'),
			{ eval: true, workerData: { path: dbPath } }
		);
		worker.once('message', (event: { ready?: boolean }) => {
			if (event.ready) resolve(worker);
		});
		worker.once('error', reject);
	});
}

function waitForMessage(
	worker: Worker,
	predicate: (msg: Record<string, unknown>) => boolean
): Promise<void> {
	return new Promise((resolve) => {
		const handler = (msg: Record<string, unknown>) => {
			if (predicate(msg)) {
				worker.off('message', handler);
				resolve();
			}
		};
		worker.on('message', handler);
	});
}

async function run(): Promise<void> {
	const worker = await spawnWorker();

	for (let round = 0; round < ROUNDS; round++) {
		// Open M's anchor handle so the descriptor refcount is 2 when W opens.
		const db = RocksDatabase.open(dbPath);

		// Tell W to start this round: W opens the DB and begins a commit.
		worker.postMessage({ nextRound: true });

		// Wait for W to close its handle and signal us.
		await waitForMessage(worker, (msg) => Boolean(msg.committing));

		// Closing M's handle drops the last descriptor ref → descriptor->close()
		// → T->close() PATH A → waitForAsyncWorkCompletion + test-seam sleep.
		db.close();

		// Wait for W's complete callback to finish the round.
		await waitForMessage(worker, (msg) => Boolean(msg.done));

		// The descriptor is now fully closed and erased from the registry.
		// Signal W that it may open a fresh DB handle for the next round.
		// (Not sent on the last round — W gets "stop" instead.)
	}

	// Stop the worker cleanly.
	await new Promise<void>((resolve) => {
		waitForMessage(worker, (msg) => Boolean(msg.stopped)).then(resolve);
		worker.postMessage({ stop: true });
	});
	await worker.terminate();
}

try {
	await run();
	console.log('SUCCESS');
	process.exit(0);
} catch (error) {
	console.error('FAILED', error);
	process.exit(1);
}
