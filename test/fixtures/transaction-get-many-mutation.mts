import { RocksDatabase, Transaction } from '../../src/index.ts';
import assert from 'node:assert/strict';

const [path, mode, route] = process.argv.slice(2);
const db = RocksDatabase.open(path, { pessimistic: mode === 'pessimistic' });
const target =
	route === 'routed'
		? RocksDatabase.open(path, { name: 'other', pessimistic: mode === 'pessimistic' })
		: db;
for (const key of ['cold', 'staged', 'deleted']) target.putSync(key, `original-${key}`);
target.flushSync();
if (target !== db) target.close();
db.close();
db.open();
if (target !== db) target.open();

const txn = new Transaction(db.store, { disableSnapshot: mode === 'no-snapshot' });
try {
	const options = { transaction: txn };
	target.putSync('staged', 'staged-before', options);
	target.removeSync('deleted', options);
	const keys = ['cold', 'staged', 'deleted', 'cold'];
	const pending = route === 'routed' ? target.getMany(keys, options) : txn.getMany(keys);
	assert.ok(pending instanceof Promise, 'must exercise the queued worker');
	if (mode === 'commit' || mode === 'commit-sync') {
		const outcome = pending.then(
			(values) =>
				assert.deepEqual(values, ['original-cold', 'staged-before', undefined, 'original-cold']),
			(error) =>
				assert.match(error.message, /Database closed during transaction get many operation/)
		);
		if (mode === 'commit-sync') txn.commitSync();
		else await txn.commit();
		await outcome;
	} else {
		if (mode === 'reset') {
			target.putSync('cold', 'concurrent');
			target.putSync('cold', 'conflicting', options);
			assert.throws(() => txn.commitSync(), /busy|conflict/i);
			// A reset must not change the snapshot or staged values of the pending read.
			target.putSync('staged', 'after-reset', options);
		} else {
			target.putSync('cold', 'staged-after', options);
			target.putSync('deleted', 'reinserted', options);
			target.removeSync('staged', options);
		}
		assert.deepEqual(await pending, ['original-cold', 'staged-before', undefined, 'original-cold']);
	}
} finally {
	if (mode !== 'commit' && mode !== 'commit-sync') txn.abort();
	if (target !== db) target.close();
	db.close();
}
