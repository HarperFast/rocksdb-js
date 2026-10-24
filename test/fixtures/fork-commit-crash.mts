import { RocksDatabase, Transaction } from '../../src/index.ts';
import { setCommitCrashPointForTesting } from '../../src/load-binding.ts';
import { writeSync } from 'node:fs';

// Dies inside a logged commit at the named point (1 = after the log write,
// 2 = after the RocksDB commit, 3 = after a pre-log rejection) so the parent
// can inspect what a reopen sees.
//   argv: <dbPath> <1|2|3>
const point = Number(process.argv[3]);
if (process.argv.length < 4 || ![1, 2, 3].includes(point)) {
	throw new Error('usage: fork-commit-crash.mts <dbPath> <1|2|3>');
}

const db = RocksDatabase.open(process.argv[2]);
const log = db.useLog('audit');
await db.transaction(async (txn) => {
	txn.putSync('seed', 'seed');
	log.addEntry(Buffer.from('seed'), txn.id);
});
// txn.state now names the seed entry's end; the crash entry lands after it.
db.flushSync();
const expectedPosition = log.getStats().nextLogPosition;

const txn = new Transaction(db.store);
if (point === 3) {
	await db.put('k', 'initial');
	await txn.get('k');
	await db.put('k', 'conflict');
}
txn.putSync('k', 'crashed');
log.addEntry(Buffer.from('crash-entry'), txn.id);

writeSync(1, `${JSON.stringify({ expectedPosition })}\nready\n`);
setCommitCrashPointForTesting(point as 1 | 2 | 3);
await txn.commit();
writeSync(2, 'commit returned; the crash point did not fire\n');
process.exit(2);
