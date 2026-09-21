import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// Regression test for db_registry.cpp:603/:655 (OpenDB's per-key condition
// waits): a writable and a read-only descriptor on one path -- exactly what
// DestroyDB/Shutdown close sequentially -- quarantine, then both retry under
// one shutdown() call. shutdown() claims and marks BOTH closeRetrying=true
// under a single lock, then closes them one at a time, so an opener racing in
// has two descriptors closing concurrently with staggered completion. The
// buggy predecessor parked on the FIRST selected entry's condition variable
// but re-scanned the whole path in its predicate: when that entry finished
// and notified, the predicate saw the second entry still closing and slept
// again -- on a condition nothing would ever notify again -- stalling the
// opener to the full lifecycleWaitSeconds deadline. The fix tracks only the
// selected entry and re-selects after each wake, so open() should succeed
// once both retries finish, not at the deadline.
RocksDatabase.config({ lifecycleWaitSeconds: 8 });

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
// Open the second descriptor BEFORE quarantining anything: once one entry on
// a path is quarantined, OpenDB rejects opening any other key on that same
// path too (it scans by path, not by exact key).
const readOnly = RocksDatabase.open(path, { readOnly: true });

try {
	db.close();
	throw new Error('Expected the writable close to fail');
} catch (error) {
	if (!String(error).includes('Injected database close flush failure')) throw error;
}
try {
	readOnly.close();
	throw new Error('Expected the read-only close to fail');
} catch (error) {
	if (!String(error).includes('Injected database close flush failure')) throw error;
}

function bothQuarantined(): boolean {
	const entries = registryStatus().filter((entry) => entry.path === path);
	return entries.length === 2 && entries.every((entry) => entry.closeError && !entry.closeRetrying);
}
for (let attempt = 0; attempt < 40 && !bothQuarantined(); attempt++) await delay(25);
if (!bothQuarantined()) throw new Error('Both descriptors did not quarantine');

const worker = new Worker(createWorkerBootstrapScript('./test/workers/shutdown-retry-worker.mts'), {
	eval: true,
});
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}
await nextMessage(); // worker started, about to call shutdown()
const shutdownResult = nextMessage();

function bothRetrying(): boolean {
	const entries = registryStatus().filter((entry) => entry.path === path);
	return entries.length === 2 && entries.every((entry) => entry.closeRetrying);
}
for (let attempt = 0; attempt < 40 && !bothRetrying(); attempt++) await delay(25);
if (!bothRetrying()) throw new Error('Both retries were never claimed');

const start = Date.now();
// This handle is only for timing -- shutdown() is process-wide, not scoped to
// this path, and its worker call may still be looping (re-scanning for
// anything left to close) after this open() returns but before it posts
// `shutdownResult`. A handle opened while that loop can still run is not
// safe to read from or hold onto: shutdown() would sweep and force-close it
// too. Let it go and reopen fresh below, strictly after the worker (and its
// in-flight shutdown() call) is confirmed done.
RocksDatabase.open(path);
const elapsed = Date.now() - start;
// Two sequential ~1.5s retries: open() must wait past the first (proves it
// didn't just get lucky reopening as soon as ITS selected entry finished) but
// comfortably clear of the 8s deadline (proves it did not stall to it).
if (elapsed < 1200 || elapsed > 5000) {
	throw new Error(`Open did not wait out both sequential retries correctly (${elapsed}ms)`);
}

const result = await shutdownResult;
if (!result.shutdown) throw new Error(`Shutdown retry failed: ${JSON.stringify(result)}`);
await worker.terminate();

// Now safe: the worker and its shutdown() call are both fully done.
const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value') throw new Error('Retry did not preserve data');
reopened.destroy();
