/**
 * Run with ROCKSDB_JS_ITERATOR_POOL=0: transaction ranges never pool and still
 * return the same rows.
 */
import { RocksDatabase } from '../../src/index.ts';
import { iteratorPoolEnabled, iteratorPoolStats } from '../../src/load-binding.ts';
import { Transaction } from '../../src/transaction.ts';

const db = RocksDatabase.open(process.argv[2]!);
for (const key of ['a', 'b', 'c']) {
	db.putSync(key, key);
}
const txn = new Transaction(db.store);
const rows: unknown[] = [];
for (let i = 0; i < 3; i++) {
	rows.push(txn.getRange({ start: 'b' }).asArray.map((entry) => entry.key));
}
txn.abort();
db.close();
console.log(JSON.stringify({ enabled: iteratorPoolEnabled, stats: iteratorPoolStats(), rows }));
