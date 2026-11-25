import {
	RocksDatabase,
	registryStatus,
	shutdown,
	steadyClockNow,
	Transaction,
} from '../../src/index.ts';
import assert from 'node:assert/strict';

// ROCKSDB_JS_CLOSE_FLUSH_FAILURE=1 makes shutdown() quarantine the descriptor before its closables
// sweep, and ROCKSDB_JS_TXN_GET_DELAY_MS parks each transaction read. abort() then finds two reads
// in flight on a closing descriptor that no sweep will reach. It must return without waiting for
// them, and the transaction must close once the second one settles, before any shutdown() retry.
// READ picks the read: `get` or `getMany`.
const path = process.argv[2];
const read = process.env.READ;
if (!path || (read !== 'get' && read !== 'getMany')) {
	console.error('Usage: READ=<get|getMany> node fork-abort-get-quarantine.mts <dbPath>');
	process.exit(1);
}
if (Number(process.env.ROCKSDB_JS_CLOSE_FLUSH_FAILURE) !== 1) {
	throw new Error('ROCKSDB_JS_CLOSE_FLUSH_FAILURE must be 1');
}
const getDelayMs = Number(process.env.ROCKSDB_JS_TXN_GET_DELAY_MS);
if (!(getDelayMs >= 1500)) {
	throw new Error('ROCKSDB_JS_TXN_GET_DELAY_MS must be at least 1500');
}
const staggerMs = getDelayMs / 3;

const db = RocksDatabase.open(path);
await db.put('cold-1', 'value');
await db.put('cold-2', 'value');
// Flushed and never read, so the data block is not cached and each read goes to a libuv worker.
await db.flush();

const txn = new Transaction(db.store);
txn.putSync('staged', 'value');
function startRead(key: string): Promise<string> {
	// One key would take the single-get path; the staged key resolves from the batch.
	const pending = read === 'get' ? txn.get(key) : txn.getMany([key, 'staged']);
	assert.ok(pending instanceof Promise, 'expected a cold-cache async read');
	return pending.then(
		() => 'resolved',
		(error: Error) => error.message
	);
}

const issuedAt = steadyClockNow();
const first = startRead('cold-1');
const sleepCell = new Int32Array(new SharedArrayBuffer(4));
Atomics.wait(sleepCell, 0, 0, staggerMs);
const secondIssuedAt = steadyClockNow();
const second = startRead('cold-2');

assert.throws(() => shutdown(), /Injected database close flush failure/);
const quarantined = registryStatus().find((entry) => entry.path === path);
assert.ok(quarantined?.closeError, 'the flush failure did not quarantine the database');
assert.equal(quarantined.transactions, 1);

const started = steadyClockNow();
assert.ok(started - issuedAt < getDelayMs - 200, `abort() started ${started - issuedAt}ms late`);
txn.abort();
const elapsed = steadyClockNow() - started;
assert.ok(elapsed < staggerMs, `abort() waited ${elapsed}ms for the in-flight reads`);
assert.throws(() => (txn as any)._context.commitSync(), { code: 'ERR_WRITES_ABANDONED' });

const firstOutcome = await first;
// A close on the first completion would drain the second read on this thread before it settles.
const firstSettled = steadyClockNow() - secondIssuedAt;
assert.ok(
	firstSettled < getDelayMs - 100,
	`the first read settled ${firstSettled}ms after the second was issued`
);
assert.equal(
	registryStatus().find((entry) => entry.path === path)?.transactions,
	1,
	'the transaction closed while its second read was still in flight'
);
const secondOutcome = await second;
const after = registryStatus().find((entry) => entry.path === path);
assert.equal(
	after?.transactions,
	0,
	`the aborted transaction stayed registered: ${JSON.stringify(after)}`
);
const cancelled =
	read === 'get'
		? /Database closed during transaction get operation/
		: /Database closed during transaction get many operation/;
assert.match(firstOutcome, cancelled);
assert.match(secondOutcome, cancelled);
txn.abort();
assert.throws(() => (txn as any)._context.commitSync(), { code: 'ERR_ALREADY_ABORTED' });

shutdown();
assert.equal(
	registryStatus().some((entry) => entry.path === path),
	false,
	'the retried shutdown did not release the database'
);
const reopened = RocksDatabase.open(path);
try {
	assert.equal(reopened.getSync('cold-1'), 'value');
	assert.equal(reopened.getSync('staged'), undefined);
} finally {
	reopened.close();
}
console.log(JSON.stringify({ ok: true }));
