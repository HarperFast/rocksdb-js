// Runs in its own process: the verification table is process-global, and these scenarios need a
// process in which nothing has created it yet. `delayed-read` also needs
// ROCKSDB_JS_TXN_GET_DELAY_MS, which is read once at process start.
import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { constants } from '../../src/load-binding.ts';
import assert from 'node:assert/strict';

const { FRESH_VERSION_FLAG } = constants;
const [scenario, path] = process.argv.slice(2);
assert.ok(scenario && path);

const v1 = 1.5e12;
const v2 = 1.6e12;
const valueAt = (version: number): Buffer => {
	const value = Buffer.alloc(16);
	value.writeDoubleBE(version, 0);
	return value;
};
const versionOf = (value: unknown): number => (value as Buffer).readDoubleBE(0);

// No block cache, so the first, cache-only attempt of a get() of a flushed key always misses.
const db = RocksDatabase.open(path, {
	encoding: false,
	verificationTable: true,
	noBlockCache: true,
	enableStats: true,
});
const key = Buffer.from('key');
db.putSync(key, valueAt(v1));
db.flushSync();

async function readCold(read: () => any): Promise<number> {
	const result = read();
	assert.ok(result instanceof Promise, 'expected a thread-pool read');
	return versionOf(await result);
}

if (scenario === 'txn-first') {
	// The first VT use in the process is a transactional populate read.
	const txn = new Transaction(db.store);
	assert.equal(await readCold(() => txn.getBinary(key, { populateVersion: true } as any)), v1);
	txn.abort();
	const keysRead = db.getStat('rocksdb.number.keys.read');
	assert.equal(db.getBinary(key, { expectedVersion: v1 } as any), FRESH_VERSION_FLAG);
	assert.equal(db.getStat('rocksdb.number.keys.read'), keysRead);
} else if (scenario === 'staged-write') {
	// A write staged before any read touches the VT must still invalidate what a read publishes.
	const writer = new Transaction(db.store);
	writer.putSync(key, valueAt(v2));
	assert.equal(await readCold(() => db.getBinary(key, { populateVersion: true } as any)), v1);
	writer.commitSync();
	const result = await db.getBinary(key, { expectedVersion: v1 } as any);
	assert.notEqual(result, FRESH_VERSION_FLAG);
	assert.equal(versionOf(result), v2);
} else if (scenario === 'delayed-read') {
	// The write lands after the read observed the slot and before it reads RocksDB.
	const txn = new Transaction(db.store);
	const pending = txn.getBinary(key, { populateVersion: true } as any);
	assert.ok(pending instanceof Promise, 'expected a thread-pool read');
	db.putSync(key, valueAt(v2));
	assert.equal(versionOf(await pending), v1);
	txn.abort();
	const result = await db.getBinary(key, { expectedVersion: v1 } as any);
	assert.notEqual(result, FRESH_VERSION_FLAG);
	assert.equal(versionOf(result), v2);
} else {
	throw new Error(`unknown scenario ${scenario}`);
}

db.close();
shutdown();
console.log('ok');
