import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const original = RocksDatabase.open(path);
for (let i = 0; i < 20; i++) {
	original.putSync(`key-${i}`, i);
}

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

// 20 rows x ROCKSDB_JS_COUNT_DELAY_MS holds an OperationGuard well past the
// worker's 50ms destroy delay. finishClose() drains in-flight operations with
// an untimed wait, so without the isClosing() poll in countRemaining() this
// scan would run to completion, block the destroy for its full duration, and
// report a count of a database that is being deleted.
const started = Date.now();
let countError: unknown;
try {
	original.getKeysCount({});
} catch (error) {
	countError = error;
}
const elapsed = Date.now() - started;

if (!countError)
	throw new Error(`Expected getKeysCount to abort, but it returned after ${elapsed}ms`);
if (!String(countError).includes('Database is closing')) throw countError;
if (elapsed >= 1000) {
	throw new Error(`getKeysCount ran ${elapsed}ms; it should abort rather than finish the scan`);
}

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);

if (registryStatus().some((entry) => entry.path === path)) {
	throw new Error('Expected destroy to fully clear the registry entry');
}

await worker.terminate();
