/**
 * Repro for the user-shared-buffer finalize use-after-free observed during
 * Harper shutdown (ctrl-C) on macOS. The reported crash:
 *
 *   userSharedBufferFinalize -> napi_get_reference_value  (EXC_BAD_ACCESS)
 *   ... BufferFinalizer::FinalizeBufferCallback
 *   ... Environment::RunCleanup
 *   ... FreeEnvironment (worker / process teardown)
 *
 * Scenario (a graceful shutdown while a shared buffer is still referenced):
 * 1. Open a DB and get one or more user-shared ArrayBuffers WITH a callback, so
 *    a listener (napi_ref + threadsafe function) is registered for each.
 * 2. Shut the DB down (`db.close()` -> removeListenersByOwner releases each
 *    listener's tsfn, whose finalizer deletes the backing napi_ref).
 * 3. Keep the ArrayBuffers referenced and let the process exit NATURALLY (no
 *    `process.exit()`), so Environment::RunCleanup finalizes the still-live
 *    external ArrayBuffers.
 *
 * `userSharedBufferFinalize` then reads `finalizeData->callbackRef` via
 * `napi_get_reference_value` -- but that ref's ownership was transferred to the
 * (now torn-down) listener tsfn in `addListener`, so it has already been
 * deleted: a use-after-free. On a plain build the freed slot usually reads back
 * benign; under an instrumented allocator (Guard Malloc on macOS, ASan on
 * Linux) it faults, which is how the test surfaces it deterministically.
 *
 * Exit 0 = survived (expected once the finalizer stops touching the borrowed
 * callbackRef); a crash/non-zero exit = the UAF reproduced.
 */
import { RocksDatabase } from '../../src/index.ts';
import { mkdirSync } from 'node:fs';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-user-shared-buffer-shutdown.mts <dbPath>');
	process.exit(1);
}

mkdirSync(dbPath, { recursive: true });

const db = RocksDatabase.open(dbPath, { name: 'shutdown-cf' });

// Retain several buffers-with-callbacks so they are still "in use" at shutdown.
const held: ArrayBuffer[] = [];
for (let i = 0; i < 16; i++) {
	const buffer = db.getUserSharedBuffer(`shutdown-key-${i}`, new ArrayBuffer(8), {
		callback() {
			// presence of the listener is what matters, not its body
		},
	});
	new DataView(buffer).setBigUint64(0, 1n);
	held.push(buffer);
}
// Keep them reachable past `db.close()` so teardown must finalize them.
(globalThis as unknown as { __heldBuffers?: ArrayBuffer[] }).__heldBuffers = held;

// Graceful shutdown: tear the DB down while the buffers are still referenced.
db.close();

// Intentionally no `process.exit()`: allow a natural exit so the finalizers run
// during Environment::RunCleanup -- the crashing path from the report.
console.log('SUCCESS');
