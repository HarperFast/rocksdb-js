/**
 * With a stalling WriteBufferManager, transaction ranges must not pool: a
 * synchronous write can block the JS thread waiting for memory that idle
 * pooled iterators pin, and only that thread can release them. Prints the
 * pool counters for a run with stalls on and one with them switched off at
 * runtime.
 */
import { RocksDatabase } from '../../src/index.ts';
import { iteratorPoolStats } from '../../src/load-binding.ts';
import { Transaction } from '../../src/transaction.ts';

const dbPath = process.argv[2];
if (!dbPath) {
	throw new Error('Missing database path');
}

RocksDatabase.config({
	writeBufferManagerSize: 64 * 1024 * 1024,
	writeBufferManagerAllowStall: true,
});
const db = RocksDatabase.open(dbPath);
db.putSync('a', 'a');

function run(): { created: number; reused: number } {
	const before = iteratorPoolStats();
	const txn = new Transaction(db.store);
	for (let i = 0; i < 3; i++) {
		if (txn.getRange().asArray.length !== 1) {
			throw new Error('wrong rows');
		}
	}
	txn.abort();
	const after = iteratorPoolStats();
	return { created: after.created - before.created, reused: after.reused - before.reused };
}

const stalling = run();
RocksDatabase.config({ writeBufferManagerAllowStall: false });
const notStalling = run();
db.close();
console.log(JSON.stringify({ stalling, notStalling }));
