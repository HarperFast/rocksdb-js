import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// The async shape of fork-compact-cancel-sync.mts. An async compact() releases
// its OperationGuard at setup handoff, so it is not covered by finishClose()'s
// in-flight drain -- it is awaited later, by DBHandle::close() during the
// closables sweep. Both shapes therefore depend on the token being armed by the
// close claim rather than at any point inside finishClose().
//
// Mutation coverage differs between the two shapes, deliberately: unwiring
// options.canceled fails both, while moving the arm past the in-flight drain
// only fails the sync fixture (an async compaction is not counted by that
// drain, so a late arm still beats the closables sweep).
const path = process.argv[2];
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

// Start the compaction first and let the libuv worker pick it up, so it is
// parked in the seam *before* the close claim. Otherwise the execute callback's
// own opened()/isCancelled() check would reject it and the token would never be
// exercised -- which the assertion below distinguishes.
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
if (compactSettled) throw new Error('Compaction settled before the close claim; seam not active');

const started = Date.now();
worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Worker did not start destroying: ${JSON.stringify(destroying)}`);

const compactError = await outcome;
const elapsed = Date.now() - started;

if (!compactError)
	throw new Error(
		`Expected compact() to be cancelled, but it resolved after ${elapsed}ms -- ` +
			'the cancel token is no longer reaching rocksdb::CompactRangeOptions::canceled'
	);
if (String(compactError).includes('Database closed during compact operation'))
	throw new Error(
		'Compaction was rejected at entry rather than cancelled mid-flight; the fixture ' +
			'no longer exercises the cancel token'
	);
if (!/cancel|paused|incomplete/i.test(String(compactError))) throw compactError;
if (elapsed >= 2000)
	throw new Error(
		`compact() ran ${elapsed}ms after the close claim; it should have been cancelled immediately`
	);

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);

if (registryStatus().some((entry) => entry.path === path))
	throw new Error('Expected destroy to fully clear the registry entry');

await worker.terminate();
