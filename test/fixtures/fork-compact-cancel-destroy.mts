import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// A foreign destroy must cancel an async compaction *before* the first step of
// finishClose() that blocks on it -- not when the closables sweep eventually
// closes the owning handle.
//
// finishClose() runs: in-flight wait (an async compact holds no OperationGuard,
// so this returns immediately) -> flush -> the optional compactOnClose pass,
// which takes DBDescriptor::compactMutex -> WaitForCompact(), which does not
// return while a manual compaction is running -> only then the closables sweep.
// The running compaction holds compactMutex, so with cancellation deferred to
// the sweep the destroy parks for the compaction's full remaining duration
// while holding destroyingPaths -- and every concurrent open of the path then
// times out after lifecycleWaitSeconds.
//
// compactOnClose is enabled here to make that ordering observable with the
// existing seam: the parked compaction holds compactMutex, so a late arm shows
// up as a slow destroy. fork-compact-cancel-async.mts cannot see it -- the seam
// parks before db->CompactRange, so with compactOnClose off nothing between the
// in-flight wait and the sweep touches the compaction.
const path = process.argv[2];
RocksDatabase.config({ compactOnClose: true });
const db = RocksDatabase.open(path);
for (let i = 0; i < 20; i++) {
	db.putSync(`key-${i}`, i);
}
db.flushSync();

const worker = new Worker(createWorkerBootstrapScript('./test/workers/destroy-open-worker.mts'), {
	eval: true,
	workerData: { path, destroyStartDelayMs: 0 },
});

function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready) throw new Error(`Worker failed to initialize: ${JSON.stringify(ready)}`);

const compacting = db.compact();
let compactSettled = false;
const outcome = compacting.then(
	() => {
		compactSettled = true;
		return null;
	},
	(error: unknown) => {
		compactSettled = true;
		return error;
	}
);
await delay(250);
if (compactSettled) throw new Error('Compaction settled before the destroy claim; seam not active');

const started = Date.now();
worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Worker did not start destroying: ${JSON.stringify(destroying)}`);

const destroyResult = await nextMessage();
const elapsed = Date.now() - started;
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);
if (elapsed >= 2500)
	throw new Error(
		`destroy() took ${elapsed}ms while an async compact() was in flight -- ` +
			'finishClose() is not cancelling attached handles before it blocks on them'
	);

const compactError = await outcome;
if (!compactError)
	throw new Error('Expected the in-flight compact() to be cancelled by the foreign destroy');
if (String(compactError).includes('Database closed during compact operation'))
	throw new Error(
		'Compaction was rejected at entry rather than cancelled mid-flight; the fixture ' +
			'no longer exercises the cancel token'
	);
if (!/cancel|paused|incomplete/i.test(String(compactError))) throw compactError;

if (registryStatus().some((entry) => entry.path === path))
	throw new Error('Expected destroy to fully clear the registry entry');

await worker.terminate();
