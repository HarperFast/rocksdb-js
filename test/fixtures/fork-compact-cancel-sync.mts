import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// Regression coverage for the manual-compaction cancellation contract (see
// DBDescriptor::compactCancelRequested). compactSync() holds an OperationGuard
// for the whole compaction, and finishClose() drains those guards with an
// untimed wait -- so a compaction that is not told to cancel blocks the close,
// and with it every concurrent open of the path.
//
// ROCKSDB_JS_COMPACT_DELAY_MS parks the compaction inside that guard until the
// token is armed (or the bound expires), which removes the dependency on how
// long a real compaction happens to run. The two assertions below fail for the
// two ways this can regress: a slow close means arming moved after the drain,
// and a successful compaction means the token stopped reaching RocksDB.
const path = process.argv[2];
const db = RocksDatabase.open(path);
for (let i = 0; i < 20; i++) {
	db.putSync(`key-${i}`, i);
}
db.flushSync();

const worker = new Worker(createWorkerBootstrapScript('./test/workers/destroy-open-worker.mts'), {
	eval: true,
	workerData: { path, destroyStartDelayMs: 50 },
});

function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready) throw new Error(`Worker failed to initialize: ${JSON.stringify(ready)}`);

worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Worker did not start destroying: ${JSON.stringify(destroying)}`);

const started = Date.now();
let compactError: unknown;
try {
	db.compactSync();
} catch (error) {
	compactError = error;
}
const elapsed = Date.now() - started;

if (!compactError)
	throw new Error(
		`Expected compactSync to be cancelled, but it succeeded after ${elapsed}ms -- ` +
			'either the token is armed after finishClose() drains in-flight operations, ' +
			'or it no longer reaches rocksdb::CompactRangeOptions::canceled'
	);
if (!/cancel|paused|incomplete/i.test(String(compactError))) throw compactError;
if (elapsed >= 2000)
	throw new Error(
		`compactSync ran ${elapsed}ms; the close claim should have cancelled it immediately`
	);

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);

if (registryStatus().some((entry) => entry.path === path))
	throw new Error('Expected destroy to fully clear the registry entry');

await worker.terminate();
