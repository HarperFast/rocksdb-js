import { RocksDatabase, registryStatus, steadyClockNow, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';

// ROCKSDB_JS_TXN_GET_DELAY_MS parks the transaction's async get, admitted and
// registered, before its worker checks for cancellation. ABORT_DURING selects
// whether the foreign destroy begins while abort() waits on that get
// (`admitted`) or has already published `closing` when abort() runs (`closing`).
const path = process.argv[2];
const mode = process.env.ABORT_DURING;
if (!path || (mode !== 'admitted' && mode !== 'closing')) {
	console.error('Usage: ABORT_DURING=<admitted|closing> node fork-abort-get-destroy.mts <dbPath>');
	process.exit(1);
}

const db = RocksDatabase.open(path);
await db.put('cold', 'value');
await db.flush();
// A cold block cache sends the transaction's get to a libuv worker.
db.close();
db.open();

const worker = new Worker(createWorkerBootstrapScript('./test/workers/destroy-open-worker.mts'), {
	eval: true,
	workerData: { path, destroyStartDelayMs: mode === 'admitted' ? 100 : 0 },
});

function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready) throw new Error(`Worker failed to initialize: ${JSON.stringify(ready)}`);

const txn = new Transaction(db.store);
txn.putSync('staged', 'value');
const issuedAt = steadyClockNow();
const read = txn.get('cold');
assert.ok(read instanceof Promise, 'expected a cold-cache async read');
const settled = read.then(
	() => 'resolved',
	(error: Error) => error.message
);

worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying) {
	throw new Error(`Worker did not start destroying: ${JSON.stringify(destroying)}`);
}

if (mode === 'closing') {
	const deadline = Date.now() + 5000;
	while (db.isOpen()) {
		if (Date.now() > deadline) throw new Error('Foreign destroy never published closing');
	}
}

const started = Date.now();
txn.abort();
const elapsed = Date.now() - started;
if (mode === 'closing') {
	// Teardown owns this rollback; abort() must not wait on the parked get.
	assert.ok(elapsed < 500, `abort() blocked ${elapsed}ms after teardown began`);
}

assert.match(await settled, /Database closed during transaction get operation/);

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);
if (mode === 'admitted') {
	// The abort's operation guard holds the destroy until the parked get is done.
	const destroyAfterGet = destroyResult.destroyedAt - issuedAt;
	assert.ok(
		destroyAfterGet >= 950,
		`destroy finished ${destroyAfterGet}ms after the get was issued`
	);
}
assert.equal(
	registryStatus().some((entry) => entry.path === path),
	false,
	'Expected destroy to fully clear the registry entry'
);

await worker.terminate();
