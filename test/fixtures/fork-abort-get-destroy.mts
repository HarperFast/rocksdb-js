import { RocksDatabase, registryStatus, steadyClockNow, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';

// ROCKSDB_JS_TXN_GET_DELAY_MS parks the transaction's async get, admitted and
// registered, before its worker checks for cancellation. ABORT_DURING picks the
// race: `admitted` starts the foreign destroy while abort() waits on that get,
// `owner-closed` does the same after this thread closed its own handle, and
// `closing` runs abort() after the destroy has already published `closing`.
const path = process.argv[2];
const mode = process.env.ABORT_DURING;
if (!path || (mode !== 'admitted' && mode !== 'owner-closed' && mode !== 'closing')) {
	console.error(
		'Usage: ABORT_DURING=<admitted|owner-closed|closing> node fork-abort-get-destroy.mts <dbPath>'
	);
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
	workerData: { path, destroyStartDelayMs: mode === 'closing' ? 0 : 500 },
});
const messages: any[] = [];
const waiters: ((message: any) => void)[] = [];
worker.on('message', (message) => {
	const waiter = waiters.shift();
	if (waiter) waiter(message);
	else messages.push(message);
});
function nextMessage(): Promise<any> {
	if (messages.length > 0) return Promise.resolve(messages.shift());
	return new Promise((resolve, reject) => {
		waiters.push(resolve);
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
if (mode === 'owner-closed') {
	// The worker's handle keeps the database open; this handle's descriptor is gone.
	db.close();
}

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

const started = steadyClockNow();
txn.abort();
const abortedAt = steadyClockNow();
if (mode === 'closing') {
	assert.ok(
		abortedAt - started < 500,
		`abort() blocked ${abortedAt - started}ms after teardown began`
	);
	// The deferred transaction stays Pending; the closing database refuses it.
	assert.throws(() => txn.putSync('later', 'value'), /Database is closing/);
	assert.throws(() => txn.commitSync(), /Database is closing/);
} else {
	// The destroy cannot have started yet, so abort() must take the guarded drain.
	assert.ok(started - issuedAt < 500, `abort() started ${started - issuedAt}ms after the get`);
	assert.ok(
		abortedAt - issuedAt >= 950,
		`abort() returned ${abortedAt - issuedAt}ms after the get`
	);
}

assert.match(await settled, /Database closed during transaction get operation/);

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);
if (mode !== 'closing') {
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
