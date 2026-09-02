// Two-process clock-floor scenario, one mode per process, so the reader really
// starts from a fresh process-global clock (a close/reopen inside one process
// would be satisfied by the in-memory clock alone):
//
//   write <dbPath> <key>  — commit a transaction-log batch keyed `key` (far
//                           ahead of the wall clock) and exit
//   read  <dbPath> <key>  — open the same database and prove the clock floor
//                           was seeded above `key`
//   plain <dbPath>        — open a database with no transaction logs and prove
//                           the clock is still wall-clock time
//
// Prints one JSON line with the observations; exits 0 on success.
import { RocksDatabase } from '../../src/index.ts';

const [mode, dbPath, keyArg] = process.argv.slice(2);
const key = keyArg ? Number(keyArg) : undefined;

const db = RocksDatabase.open(dbPath);
try {
	if (mode === 'write') {
		await db.transaction(async (txn) => {
			txn.setTimestamp(key!);
			await txn.put('k', 'v');
			db.useLog('clock').addEntry(Buffer.from('entry'), txn.id);
		});
		console.log(JSON.stringify({ wrote: key }));
	} else if (mode === 'read') {
		const clock = db.getMonotonicTimestamp();
		let txnTimestamp = 0;
		await db.transaction(async (txn) => {
			txnTimestamp = txn.getTimestamp();
		});
		console.log(JSON.stringify({ clock, txnTimestamp, key, now: Date.now() }));
		if (!(clock > key!) || !(txnTimestamp > clock)) {
			process.exit(1);
		}
	} else if (mode === 'plain') {
		const now = Date.now();
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ clock, now }));
		if (!(clock > now - 1000 && clock < now + 1000)) {
			process.exit(1);
		}
	} else {
		console.error(`unknown mode ${mode}`);
		process.exit(2);
	}
} finally {
	db.close();
}
