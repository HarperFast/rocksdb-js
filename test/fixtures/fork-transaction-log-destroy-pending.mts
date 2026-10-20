import { RocksDatabase } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const dbPath = process.argv[2];
const bindVia = process.argv[3];
assert(bindVia === 'txn.useLog' || bindVia === 'log.addEntry', `unknown bind path ${bindVia}`);

const payload = Buffer.from('pending at destroy');
let db = RocksDatabase.open(dbPath);
try {
	const log = db.useLog('foo');
	const pending = db.transaction((txn) => {
		txn.putSync('key', 'value');
		if (bindVia === 'txn.useLog') {
			txn.useLog('foo').addEntry(payload, txn.id);
		} else {
			log.addEntry(payload, txn.id);
		}
	});

	const deadline = Date.now() + 5000;
	while (!isTransactionCommitExecuteDelayedForTesting()) {
		assert(Date.now() < deadline, 'commit did not reach the execute delay');
		await delay(1);
	}

	// The log batch is written and the RocksDB commit has not run: destroy must
	// leave the store and its entry in place.
	assert.deepEqual(db.purgeLogs({ destroy: true }), []);
	assert(existsSync(join(dbPath, 'transaction_logs', 'foo')), 'log store directory was removed');

	await pending;
	assert.deepEqual(
		[...log.query({ start: 0 })].map(({ data }) => data.toString()),
		[payload.toString()]
	);
} finally {
	db.close();
}

db = RocksDatabase.open(dbPath);
try {
	assert.equal(db.getSync('key'), 'value');
	assert.deepEqual(
		[...db.useLog('foo').query({ start: 0 })].map(({ data }) => data.toString()),
		[payload.toString()]
	);
} finally {
	db.close();
}
