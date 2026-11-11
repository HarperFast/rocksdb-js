// Runs in its own process: ROCKSDB_JS_TXN_GET_DELAY_MS is read once at process start.
import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import assert from 'node:assert/strict';

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

// No block cache, so a get() of a flushed key always takes the thread-pool path.
const options = { name: 'converted', encoding: false, noBlockCache: true } as const;
const plain = RocksDatabase.open(path, options);
const key = Buffer.from('key');
const other = Buffer.from('other');
plain.putSync(key, valueAt(v1));
plain.putSync(other, valueAt(v1));
plain.flushSync();

if (scenario === 'read-across-activation') {
	// A write staged without a write intent keeps the converted column family inert.
	const writer = new Transaction(plain.store);
	plain.putSync(Buffer.from('staged'), valueAt(v2), { transaction: writer });
	const cached = RocksDatabase.open(path, { ...options, verificationTable: true });

	// Created after the opt-in, so it does not hold activation back; its read is queued while the
	// column family still converts, and completes after activation.
	const reader = new Transaction(cached.store);
	const pending = reader.getBinary(key, { populateVersion: true } as any);
	assert.ok(pending instanceof Promise, 'expected a thread-pool read');

	writer.abort();
	cached.getSync(other, { populateVersion: true });
	assert.equal(cached.verifyVersion(other, v1), true, 'activated by a read after the drain');

	assert.equal(versionOf(await pending), v1);
	reader.abort();
	assert.equal(cached.verifyVersion(key, v1), false, 'a read begun while converting published');
	cached.close();
} else {
	throw new Error(`unknown scenario ${scenario}`);
}

plain.close();
shutdown();
console.log('ok');
