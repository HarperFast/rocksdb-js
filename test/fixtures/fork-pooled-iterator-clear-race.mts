/**
 * Run with ROCKSDB_JS_CLEAR_DELAY_MS: an async clear() on a libuv worker has
 * deleted the column family's files but not yet returned while this thread
 * reads in a transaction. A pooled range must return what a fresh one does.
 * Prints both row counts as JSON.
 */
import { RocksDatabase } from '../../src/index.ts';
import { Transaction } from '../../src/transaction.ts';

const FRESH = { fillCache: false };

const db = RocksDatabase.open(process.argv[2]!);
for (const key of ['a', 'b', 'c']) {
	db.putSync(key, key);
}
const txn = new Transaction(db.store);
// Leaves an idle pooled iterator. The pool is trimmed after this macrotask, so
// everything up to the pooled read below must stay synchronous.
if (txn.getRange().asArray.length !== 3) {
	throw new Error('wrong rows');
}
const cleared = db.clear();
const deadline = Date.now() + 10_000;
let fresh = txn.getRange(FRESH).asArray.length;
while (fresh !== 0) {
	if (Date.now() > deadline) {
		throw new Error('clear() did not delete the files');
	}
	fresh = txn.getRange(FRESH).asArray.length;
}
const pooled = txn.getRange().asArray.length;
await cleared;
txn.abort();
db.close();
console.log(JSON.stringify({ fresh, pooled }));
