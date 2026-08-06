/**
 * Worker body for the VT LockTracker cross-env churn repro (HarperFast/rocksdb-js#741).
 *
 * Several of these run concurrently against one DB path with the Verification
 * Table materialized, all writing a single hot key via coordinatedRetry
 * transactions so write intents constantly install/join/release on the SAME
 * LockTracker across threads. A single commit attempt per iteration (no retry
 * loop) mirrors the proven cross-thread repro shape: retrying in a loop lets
 * write-intent holders pile up on one key faster than they drain, which
 * doesn't reproduce the crash any more reliably but does make the test slow.
 *
 * Deliberately uses only Commit()/Abort() (not CommitSync/putSync/getSync) --
 * see AGENTS.md item 10a: those synchronous paths don't route through
 * tryRegisterAsyncWork(), so exercising them here wouldn't be testing this
 * fix's actual protection.
 *
 * On "stop" the worker finishes its current iteration and calls db.close()
 * itself — a GRACEFUL exit — while sibling workers sharing the same
 * process-global DBDescriptor (AGENTS.md note 6) may still be mid-commit.
 * Before the #741 fix this reliably produced a double-release on a shared
 * LockTracker (holders/refcount corruption) or a UAF on TransactionHandle::txn.
 */
import { RocksDatabase } from '../../src/index.js';
import { RETRY_NOW, Transaction } from '../../src/transaction.js';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath, id } = workerData as { dbPath: string; id: number };

const HOT_KEY = Buffer.from('vt-churn-hot-row');
const versioned = (v: number): Buffer => {
	const buf = Buffer.alloc(16);
	buf.writeDoubleBE(v, 0);
	return buf;
};

const db = RocksDatabase.open(dbPath, { encoding: false, verificationTable: true });
await db.put(HOT_KEY, versioned(1.5e12));
db.populateVersion(HOT_KEY, 1.5e12); // materialize the VT slot -- write intents now engage

let stopping = false;
parentPort?.on('message', (m: unknown) => {
	if (m === 'stop') stopping = true;
});

for (let i = 0; !stopping; i++) {
	const role = i % 7;
	try {
		if (role === 6) {
			// Holder: install intent, hold briefly, then settle -- the shape a
			// killed HTTP client leaves behind if teardown races it mid-hold.
			const t = new Transaction(db.store, { coordinatedRetry: true });
			await t.put(HOT_KEY, versioned(2e12 + id * 1e6 + i));
			await new Promise((r) => setTimeout(r, 200));
			if (i % 2) {
				await t.commit().catch(() => {});
			} else {
				t.abort();
			}
		} else {
			// Racer: one commit attempt, abort on RETRY_NOW. No retry loop --
			// a bounded retry storm on one key piles up holders faster than
			// they drain without adding coverage (see the module doc above).
			const t = new Transaction(db.store, { coordinatedRetry: true });
			await t.put(HOT_KEY, versioned(1e12 + id * 1e6 + i));
			const r = await t.commit();
			if (r === RETRY_NOW) t.abort();
		}
	} catch {
		// Conflicts/rejections are expected under contention -- the invariant
		// under test is "no crash", not "every attempt succeeds".
	}
}

// Graceful exit: close while sibling workers may still be committing through
// the shared DBDescriptor (HarperFast/rocksdb-js#741). Exit via process.exit()
// (matching the proven repro-crossthread.mjs shape) rather than letting the
// caller separately call worker.terminate() after this -- terminate()ing a
// worker that is already mid-voluntary-exit races Node's own isolate teardown
// (observed as a V8_Fatal "array_buffer_allocator" check failure inside
// DisposeIsolate/WorkerThreadData::~WorkerThreadData, and separately as glibc
// heap-corruption aborts, in earlier versions of this fixture that did both).
db.close();
process.exit(0);
