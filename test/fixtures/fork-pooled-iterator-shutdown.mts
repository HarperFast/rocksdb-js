/**
 * `shutdown()` while transactions still hold pooled iterators. The descriptor's
 * close sweep calls each closable's `close()` with its map lock released; a
 * transaction's close detaches its pooled iterators from that same map. A
 * sweep that kept its map iterator across `close()` read a freed node.
 * `MALLOC_PERTURB_` makes that read fault instead of passing silently.
 *
 * Exit 0 = survived.
 */
import { RocksDatabase, shutdown } from '../../src/index.ts';
import { Transaction } from '../../src/transaction.ts';

const dbPath = process.argv[2];
if (!dbPath) {
	throw new Error('Missing database path');
}

const db = RocksDatabase.open(dbPath);
for (const key of ['a', 'b', 'c']) {
	db.putSync(key, key);
}
const retained: unknown[] = [];
for (let i = 0; i < 32; i++) {
	const txn = new Transaction(db.store);
	retained.push(txn);
	for (let j = 0; j < 3; j++) {
		txn.getRange().asArray;
		const busy = txn.getRange()[Symbol.iterator]();
		busy.next();
		retained.push(busy);
	}
}
shutdown();
console.log(`survived with ${retained.length} retained`);
