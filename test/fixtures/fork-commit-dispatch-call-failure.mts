// A commit whose completion cannot be handed to this (live) env must still settle, and must not keep
// the event loop alive: the parent kills this process on a deadline.
import { RocksDatabase, Transaction } from '../../src/index.ts';
import assert from 'node:assert/strict';

const path = process.argv[2];
if (Number(process.env.ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE) !== 2) {
	throw new Error('ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE must be 2');
}

const db = RocksDatabase.open(path);
const commit = (key: string, value: number) => {
	const txn = new Transaction(db.store);
	txn.putSync(key, value);
	return txn.commit();
};

// The first failed call retires the completion; commits that dispatch after it go through the same
// retired completion without calling its tsfn.
await Promise.all(Array.from({ length: 8 }, (_, i) => commit(`burst${i}`, i)));
// The replacement completion fails once more, alone.
await commit('alone', 1);
// And the next replacement delivers normally.
await commit('after', 2);

for (let i = 0; i < 8; i++) {
	assert.equal(db.getSync(`burst${i}`), i);
}
assert.equal(db.getSync('alone'), 1);
assert.equal(db.getSync('after'), 2);
db.close();
console.log(JSON.stringify({ ok: true }));
