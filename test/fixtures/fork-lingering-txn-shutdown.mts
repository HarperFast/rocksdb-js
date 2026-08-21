/**
 * Targeted repro for HarperFast/rocksdb-js#741: the last env's
 * DBRegistry::Shutdown() closes TransactionHandles leaked by ALREADY-DEAD
 * worker envs, and TransactionHandle::close()'s thread-identity guard passes
 * falsely when the closing thread recycled a dead leaker's pthread — so
 * napi_delete_reference writes through the dead worker's freed napi_env.
 *
 * Sequence (main thread deliberately never imports the binding, so the module
 * refcount counts workers only):
 *
 *   1. ANCHOR worker opens the DB and parks — pins the shared DBDescriptor so
 *      leaker exits never trigger the safe same-env purge/close path.
 *   2. LEAKER workers run sequentially: open DB, create a Transaction, put,
 *      exit with the transaction STILL PENDING. Each leaves a
 *      TransactionHandle in DBDescriptor::transactions whose env dangles.
 *   3. FINAL worker spawns after the last leaker dies (typically recycling
 *      its pthread, hence an equal std::thread::id) and opens the DB.
 *   4. ANCHOR exits (refcount N -> 1).
 *   5. FINAL exits last (refcount 0) -> its env-cleanup hook runs
 *      DBRegistry::Shutdown() -> finishClose() -> close() on each leaked
 *      handle. On a thread-id collision the guard passes and
 *      napi_delete_reference(freed env, freed ref) corrupts the heap.
 *
 * On glibc this surfaces as "double free or corruption" in a later free
 * (production stack: ~OptimisticTransaction -> ~TransactionBaseImpl ->
 * _int_free). On macOS the default allocator tolerates it; run under Guard
 * Malloc (DYLD_INSERT_LIBRARIES=/usr/lib/libgmalloc.dylib) to turn the write
 * into an immediate fault.
 *
 * Exit 0 = survived; crash exits via signal / non-zero.
 */
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync, rmSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];
const leakerCount = Number(process.argv[3] ?? 4);
const leakerRole = process.argv[4] ?? 'leaker';

if (!dbPath) {
	console.error('Usage: fork-lingering-txn-shutdown.mts <dbPath> [leakerCount]');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

function spawn(
	role: string,
	id: number
): { worker: Worker; exited: Promise<void>; msg: Promise<string> } {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/lingering-txn-worker.mts'),
		{ eval: true, workerData: { dbPath, role, id } }
	);
	const exited = new Promise<void>((resolve, reject) => {
		worker.once('exit', () => resolve());
		worker.once('error', reject);
	});
	const msg = new Promise<string>((resolve, reject) => {
		worker.once('message', (m: string) => resolve(m));
		worker.once('error', reject);
	});
	return { worker, exited, msg };
}

async function run(): Promise<void> {
	// 1. Anchor pins the descriptor for the whole leaker phase.
	const anchor = spawn('anchor', -1);
	await anchor.msg; // 'ready' — DB open

	// 2. Sequential leakers: each exits with a pending transaction, and the
	//    next one typically recycles its pthread.
	for (let i = 0; i < leakerCount; i++) {
		const leaker = spawn(leakerRole, i);
		await leaker.msg; // 'leaked'/'committing' — work created
		await leaker.exited; // env fully torn down before the next spawn
	}

	// 3. Final worker: spawned right after the last leaker died, so its
	//    pthread is most likely that leaker's, recycled.
	const final = spawn('final', 999);
	await final.msg; // 'ready'

	// 4. Anchor leaves first, so FINAL is the last module instance.
	anchor.worker.postMessage('stop');
	await anchor.exited;

	// 5. Final exits last -> refcount 0 -> Shutdown closes the leaked handles.
	final.worker.postMessage('stop');
	await final.exited;
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
