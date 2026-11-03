import { RocksDatabase, raiseTimestampFloor } from '../../src/index.ts';

// A child process, because the floor is process-wide: raising it an hour ahead
// in the test runner would skew every other test file's timestamps.
const [mode, dbPath, floorArg] = process.argv.slice(2);
const floor = Number(floorArg);
const LOG = 'local';

const raised = raiseTimestampFloor(floor);
const db = RocksDatabase.open(dbPath, mode === 'seeded' ? { timestampFloorLog: LOG } : {});
try {
	const clock = db.getMonotonicTimestamp();
	let txnTimestamp = 0;
	await db.transaction(async (txn) => {
		txnTimestamp = txn.getTimestamp();
		await txn.put('k', 'v');
		db.useLog(LOG).addEntry(Buffer.from('entry'), txn.id);
	});
	const logTimestamps: number[] = [];
	for (const entry of db.useLog(LOG).query({ start: 0 })) {
		logTimestamps.push(entry.timestamp);
	}
	console.log(JSON.stringify({ raised, clock, txnTimestamp, logTimestamps }));
} finally {
	db.close();
}
