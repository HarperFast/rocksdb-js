// Runs in its own process because the verification table is sized once per process: 16 slots make
// every key share a slot with many others.
import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { constants } from '../../src/load-binding.ts';
import assert from 'node:assert/strict';

const { POPULATE_VERSION_FLAG, FRESH_VERSION_FLAG } = constants;
const [path] = process.argv.slice(2);
assert.ok(path);

RocksDatabase.config({ verificationTableEntries: 16 });

const valueAt = (version: number, payload: string): Buffer => {
	const value = Buffer.alloc(8 + payload.length);
	value.writeDoubleBE(version, 0);
	value.write(payload, 8);
	return value;
};

const db = RocksDatabase.open(path, { encoding: false, verificationTable: true });
const native = (db as any).store.db;
const keys = Array.from({ length: 256 }, (_, i) => Buffer.from(`key-${i}`));
const sharedVersion = 1.5e12;
const newerVersion = 1.6e12;

// One transaction gives every key the same version, as a producer that stamps versions per
// transaction does.
const txn = new Transaction(db.store);
for (const key of keys) txn.putSync(key, valueAt(sharedVersion, 'old'));
txn.commitSync();
for (const key of keys) native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);

const [updated, ...others] = keys;
db.putSync(updated, valueAt(newerVersion, 'new'));
// Re-cache every other key. Each still holds sharedVersion, and the updated key shares a slot with
// some of them.
for (const key of others) native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);

// Colliding keys evict each other, but the last one cached still verifies. Checked first: the
// expected-version read of the updated key below publishes its new version, which evicts the last
// key whenever the two share a slot (1 in 16, by the per-process seed).
assert.equal(db.verifyVersion(others.at(-1)!, sharedVersion), true);
assert.equal(db.verifyVersion(updated, sharedVersion), false);
const read = native.getSync(updated, 0, undefined, sharedVersion);
assert.notEqual(read, FRESH_VERSION_FLAG);
assert.equal((db.getSync(updated) as Buffer).toString('utf8', 8), 'new');

db.close();
shutdown();
console.log('ok');
