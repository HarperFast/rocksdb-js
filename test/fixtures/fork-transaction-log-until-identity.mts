import { RocksDatabase, Transaction, TransactionLog } from '../../src/index.ts';
import assert from 'node:assert/strict';
import { mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';

const root = resolve(process.argv[2]);
const originalCwd = process.cwd();
const databases: RocksDatabase[] = [];
try {
	for (const name of ['a', 'b']) mkdirSync(join(root, name, 'db'), { recursive: true });
	process.chdir(join(root, 'a'));
	const first = RocksDatabase.open('db');
	databases.push(first);
	const firstLog = first.useLog('foo');
	first.transactionSync((txn) => firstLog.addEntry(Buffer.from('first'), txn.id));
	const firstCursor = firstLog.query({ start: 0 });

	process.chdir(join(root, 'b'));
	const second = RocksDatabase.open('db');
	databases.push(second);
	const secondLog = second.useLog('foo');
	const foreign = secondLog.query();
	assert.equal(firstLog.path, secondLog.path);
	assert.notEqual(first.store.db.identityPath, second.store.db.identityPath);
	assert.throws(() => firstLog.query({ start: 0, until: foreign }), TypeError);

	const alias = RocksDatabase.open(join(root, 'a', 'db'));
	databases.push(alias);
	const live = alias.useLog('foo').query();
	assert.deepEqual(
		Array.from(firstLog.query({ start: 0, until: live }), (entry) => entry.data.toString()),
		['first']
	);
	const direct = new TransactionLog(first.store.db, 'foo');
	assert(direct instanceof TransactionLog);
	assert.equal(direct.constructor, TransactionLog);
	assert.equal(firstLog.constructor, TransactionLog);
	assert.equal(Array.from(direct.query({ start: 0, until: live })).length, 1);
	class CustomLog extends TransactionLog {}
	const custom = new CustomLog(first.store.db, 'foo');
	assert(custom instanceof CustomLog);
	assert.equal(Array.from(custom.query({ start: 0, until: live })).length, 1);
	class MisnamedLog extends TransactionLog {}
	Object.defineProperty(MisnamedLog.prototype, 'name', { get: () => 'foo' });
	const different = new MisnamedLog(first.store.db, 'bar');
	assert.throws(() => firstLog.query({ until: different.query() }), TypeError);
	const txn = new Transaction(first.store);
	try {
		assert.equal(Array.from(txn.useLog('foo').query({ start: 0, until: live })).length, 1);
	} finally {
		txn.abort();
	}

	for (const db of databases.toReversed()) db.close();
	assert.equal(firstLog.path, undefined);
	assert.equal(secondLog.path, undefined);
	assert.throws(() => firstLog.query({ start: 0, until: foreign }), TypeError);
	assert.throws(() => secondLog.query({ start: 0, until: firstCursor }), TypeError);
	console.log(
		'Relative display-path collision, alias, constructor, transaction and closed-log identity passed'
	);
} finally {
	for (const db of databases.toReversed()) db.close();
	process.chdir(originalCwd);
}
