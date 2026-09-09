import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// `registryStatus()` walks `DBDescriptor::columns`, and `databasesMutex` does NOT
// cover that map: `unregisterColumnFamily()` (a `dropSync()`) erases from it under
// `columnsMutex`, and `finishClose()` clears it, both from whichever thread drives
// them. Without a snapshot under that mutex the walk reads a freed map node and
// `napi_set_named_property()` strlen()s its key -- a SIGSEGV on the JS thread.
//
// ROCKSDB_JS_REGISTRY_STATUS_COLUMNS_DELAY_MS parks the walk per column family so
// a concurrent erase lands inside it. Names are well past libstdc++'s 15-char
// small-string buffer so the erase frees a separate heap allocation the walk is
// still pointing at; an SSO name usually survives the free intact and hides it.
const path = process.argv[2];
const prefix = `registry-status-column-race-${'x'.repeat(48)}`;
const db = RocksDatabase.open(path);
db.putSync('key', 'value');

const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/column-drop-churn-worker.mts'),
	{ eval: true, workerData: { path, prefix, rounds: 40 } }
);
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}
const ready = await nextMessage();
if (!ready.ready) throw new Error(`Churn worker failed to initialize: ${JSON.stringify(ready)}`);
const churnResult = nextMessage();
worker.postMessage({ churn: true });

// Poll for the whole churn. Each call parks inside the column walk, so drops land
// mid-walk; the allocator churn makes a freed name be reused rather than left
// readable, which is what turns the use-after-free into a fault.
let polls = 0;
let sawColumns = false;
const deadline = Date.now() + 12_000;
let settled: any;
for (;;) {
	const entry = registryStatus().find((candidate) => candidate.path === path);
	polls++;
	if (entry && Object.keys(entry.columnFamilies).length > 1) sawColumns = true;
	for (let i = 0; i < 200; i++) Buffer.allocUnsafe(96).fill(0xab);
	settled = await Promise.race([churnResult, Promise.resolve(undefined)]);
	if (settled !== undefined) break;
	if (Date.now() >= deadline) throw new Error('Timed out waiting for the column churn');
	// registryStatus() holds databasesMutex through the column snapshot, so back-to-back
	// polls starve the worker's own open()s; yield between them.
	await delay(1);
}
if (settled.error) throw new Error(`Churn worker failed: ${settled.error}`);
if (!settled.churned) throw new Error(`Unexpected churn result: ${JSON.stringify(settled)}`);
if (polls < 2) throw new Error(`registryStatus() was polled only ${polls} time(s)`);
if (!sawColumns)
	throw new Error('Never observed a churned column family; the race was not exercised');
await worker.terminate();
db.destroy();
