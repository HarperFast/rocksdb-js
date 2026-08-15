/**
 * Worker body for the lingering-pending-transaction shutdown repro
 * (HarperFast/rocksdb-js#741).
 *
 * Three roles, selected via workerData.role:
 *
 * - 'anchor': opens the DB and parks until told to stop. Its only job is to
 *   pin the shared DBDescriptor so the leakers' exits never trigger the
 *   registry purge path (which would close their handles safely, on their own
 *   still-live envs).
 *
 * - 'leaker': opens the DB, creates a Transaction, puts a key, and exits with
 *   the transaction STILL PENDING — never committed, aborted, or closed. The
 *   JS Transaction finalizer only drops the JS-side shared_ptr; the
 *   DBDescriptor::transactions map keeps the TransactionHandle alive with
 *   `env` now pointing at this worker's freed napi_env and `jsDatabaseRef`
 *   pointing at a ref object freed with it. This models a worker recycled
 *   while a request's transaction was open.
 *
 * - 'final': opens the DB and exits when told. Spawned after the leakers die,
 *   so its pthread (and therefore std::thread::id) is typically a recycled
 *   leaker pthread. When it is the last module instance, its env-cleanup hook
 *   runs DBRegistry::Shutdown() -> finishClose() -> TransactionHandle::close()
 *   on the leakers' lingering handles; the "am I on the owning JS thread?"
 *   guard compares recycled thread ids, passes falsely, and
 *   napi_delete_reference writes through the dead leaker's freed env.
 */
import { RocksDatabase } from '../../src/index.ts';
import { Transaction } from '../../src/transaction.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath, role, id } = workerData as { dbPath: string; role: string; id: number };

const db = RocksDatabase.open(dbPath, { encoding: false });

if (role === 'leaker') {
	const t = new Transaction(db.store);
	await t.put(Buffer.from(`leaked-${id}`), Buffer.from(`v${id}`));
	// Exit with the transaction pending. Deliberately no commit/abort/close:
	// the handle must outlive this env inside DBDescriptor::transactions.
	parentPort?.postMessage('leaked');
	process.exit(0);
}

if (role === 'slowcommit') {
	// Start a commit and exit the env WHILE it is still executing natively.
	// ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS holds the commit inside its execute
	// phase (work still registered), so the env cleanup hook's reap hits a
	// transaction whose native work is genuinely in flight and its bounded
	// drain expires. Deliberately not awaited — the point is to die mid-commit.
	const t = new Transaction(db.store);
	await t.put(Buffer.from(`slow-${id}`), Buffer.from(`v${id}`));
	void t.commit().catch(() => {});
	// Give the commit thread time to enter execute before tearing the env down.
	await new Promise((r) => setTimeout(r, 250));
	parentPort?.postMessage('committing');
	process.exit(0);
}

if (role === 'committer') {
	// Discriminator: same shape as 'leaker' but the transaction is committed,
	// so nothing lingers in DBDescriptor::transactions. If teardown still
	// crashes with committers, the trigger is the Transaction object lifecycle
	// at env teardown, not the pending-transaction leak.
	const t = new Transaction(db.store);
	await t.put(Buffer.from(`committed-${id}`), Buffer.from(`v${id}`));
	await t.commit();
	parentPort?.postMessage('committed');
	process.exit(0);
}

// anchor / final: report ready, then park until told to stop.
parentPort?.postMessage('ready');
await new Promise<void>((resolve) => {
	parentPort?.on('message', (m: unknown) => {
		if (m === 'stop') resolve();
	});
});
process.exit(0);
