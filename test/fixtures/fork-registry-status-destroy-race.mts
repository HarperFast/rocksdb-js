import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// `registryStatus()` walks `DBDescriptor::columns`, which `databasesMutex` does
// NOT cover: `finishClose()` clears that map under `columnsMutex` from whichever
// thread drives teardown, so a cross-env destroy() can free the map node whose
// key the walk is still holding -- and `napi_set_named_property()` strlen()s it.
// ROCKSDB_JS_REGISTRY_STATUS_COLUMNS_DELAY_MS parks the walk per column family so
// the clear lands inside it; several families make the window wide.
const path = process.argv[2];
const db = RocksDatabase.open(path);
// Names well past libstdc++'s 15-char small-string buffer, so clearing the map
// frees a separate heap allocation the walk is still pointing at -- an SSO name
// usually survives the free intact and hides the bug.
const columnNames = Array.from(
	{ length: 6 },
	(_, i) => `registry-status-destroy-race-column-family-${i}-${'x'.repeat(48)}`
);
for (const name of columnNames) {
	RocksDatabase.open(path, { name }).putSync('key', name);
}
db.putSync('key', 'value');

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
if (!ready.ready) throw new Error(`Destroy worker failed to initialize: ${JSON.stringify(ready)}`);
worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Destroy worker did not start: ${JSON.stringify(destroying)}`);
const destroyResult = nextMessage();

// Poll across the whole destroy: each call parks inside the column walk, so one
// of them is guaranteed to be mid-walk when finishClose() clears the map.
const deadline = Date.now() + 10_000;
let polls = 0;
while (registryStatus().some((entry) => entry.path === path)) {
	polls++;
	// Churn the allocator so a freed column-family name is reused rather than
	// left readable, which is what turns the use-after-free into a fault.
	for (let i = 0; i < 400; i++) Buffer.allocUnsafe(96).fill(0xab);
	if (Date.now() >= deadline) throw new Error('Timed out waiting for the destroy window');
}
if (polls === 0) throw new Error('Never observed the registry entry; the race was not exercised');

const destroyed = await destroyResult;
if (!destroyed.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyed)}`);
await worker.terminate();
