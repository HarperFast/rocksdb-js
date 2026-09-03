import { RocksDatabase } from '../../src/index.ts';

// argv: <dbPath> <rounds> <valueSize>
// A primary writer in its OWN process: the configuration a follower actually
// runs against (a worker thread would share the log-store registry and the
// process's file handles with the follower). Writes a round, flushes so the
// deletions a compaction makes are real, then prints the round number so the
// parent can catch up and assert against a known state.
const [dbPath, roundsArg, valueSizeArg] = process.argv.slice(2);
const rounds = Number(roundsArg);
const value = 'v'.repeat(Number(valueSizeArg));

const db = new RocksDatabase(dbPath, { writeBufferSize: 64 * 1024 });
db.open();
try {
	// 'existing' was created before the follower opened; 'late' appears only
	// now, so the follower must not see it until it reopens.
	const existing = db.useLog('existing');
	const late = db.useLog('late');
	for (let round = 1; round <= rounds; round++) {
		for (let key = 0; key < 8; key++) {
			db.putSync(`key${key}`, `${value}${round}`);
		}
		db.putSync('round', round);
		db.transactionSync((txn) => {
			txn.put('round', round);
			existing.addEntry(Buffer.from(`existing ${round}`), txn.id);
		});
		db.transactionSync((txn) => {
			txn.put('round', round);
			late.addEntry(Buffer.from(`late ${round}`), txn.id);
		});
		db.flushSync({ allowWriteStall: true });
		process.stdout.write(`round ${round}\n`);
	}
} finally {
	db.close();
}
