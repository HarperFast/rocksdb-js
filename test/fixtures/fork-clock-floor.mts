/**
 * One mode per process, so each reader really starts from a fresh process-global
 * monotonic clock. Prints one JSON line; exits non-zero when its own assertions
 * fail.
 *
 * A backward wall-clock step across a restart is modelled by writing batch keys
 * *ahead* of the wall clock and then reopening: the durable keys are above the
 * clock the next process reads, which is the state a rollback leaves behind.
 */
import { RocksDatabase } from '../../src/index.ts';
import { readdirSync } from 'node:fs';
import { join } from 'node:path';

const [mode, dbPath, keyArg, logArg] = process.argv.slice(2);
const key = keyArg ? Number(keyArg) : undefined;
const log = logArg ?? 'local';

function fail(message: string): never {
	console.error(message);
	process.exit(1);
}

const warnings: string[] = [];
if (mode === 'warn') {
	RocksDatabase.on('log.warn', (...args: unknown[]) => warnings.push(JSON.stringify(args)));
}

const rotating = mode === 'write-rotate';
const db = RocksDatabase.open(dbPath, {
	...(mode === 'write-unseeded' ? {} : { timestampFloorLog: log }),
	// Small enough that the next batch cannot share a segment with the last one.
	...(rotating ? { transactionLogMaxSize: 64 * 1024 } : {}),
});

function segmentCount(): number {
	return readdirSync(join(dbPath, 'transaction_logs', log)).filter((name) =>
		name.endsWith('.txnlog')
	).length;
}

try {
	if (mode === 'write' || mode === 'write-unseeded' || rotating) {
		await db.transaction(async (txn) => {
			txn.setTimestamp(key!);
			await txn.put('k', 'v');
			db.useLog(log).addEntry(
				rotating ? Buffer.alloc(100 * 1024, 1) : Buffer.from('entry'),
				txn.id
			);
		});
		const segments = rotating ? segmentCount() : 0;
		console.log(JSON.stringify({ wrote: key, log, segments }));
	} else if (mode === 'read') {
		const clock = db.getMonotonicTimestamp();
		let txnTimestamp = 0;
		await db.transaction(async (txn) => {
			txnTimestamp = txn.getTimestamp();
		});
		console.log(JSON.stringify({ clock, txnTimestamp, key, now: Date.now() }));
		if (!(clock > key!)) {
			fail(`clock ${clock} did not clear the durable key ${key}`);
		}
		if (!(txnTimestamp > clock)) {
			fail(`transaction timestamp ${txnTimestamp} is not above the seeded clock ${clock}`);
		}
	} else if (mode === 'read-unseeded') {
		// Either no option at all, or one naming a different log: the keys of a log
		// this process does not originate must not move its clock.
		const clock = db.getMonotonicTimestamp();
		const now = Date.now();
		console.log(JSON.stringify({ clock, key, now }));
		if (clock > key!) {
			fail(`clock ${clock} was seeded from a log that was not named as locally originated`);
		}
		if (!(clock >= now - 60000 && clock <= now + 60000)) {
			fail(`clock ${clock} is not tracking the wall clock ${now}`);
		}
	} else if (mode === 'warn') {
		const until = Date.now() + 5000;
		while (Date.now() < until && warnings.length === 0) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ warnings, clock, key }));
		if (warnings.length === 0) {
			fail('no clock-floor warning was emitted');
		}
	} else {
		fail(`unknown mode ${mode}`);
	}
} finally {
	db.close();
}
