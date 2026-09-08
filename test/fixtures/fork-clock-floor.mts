import { RocksDatabase } from '../../src/index.ts';
import { readdirSync } from 'node:fs';
import { join } from 'node:path';

const [mode, dbPath, keyArg, logArg, expectedWarning] = process.argv.slice(2);
const key = keyArg ? Number(keyArg) : undefined;
const log = logArg ?? 'local';

function fail(message: string): never {
	console.error(message);
	process.exit(1);
}

const warnings: string[] = [];
if (mode === 'warn' || mode === 'warn-read-only' || mode === 'reopen-warn') {
	RocksDatabase.on('log.warn', (...args: unknown[]) => warnings.push(JSON.stringify(args)));
}

const rotating = mode === 'write-rotate';
const FRAME_PAYLOAD = 32;
const db = RocksDatabase.open(dbPath, {
	...(mode === 'write-unseeded' || mode === 'reopen-warn' ? {} : { timestampFloorLog: log }),
	...(mode === 'warn-read-only' ? { readOnly: true } : {}),
	...(rotating ? { transactionLogMaxSize: 64 * 1024 } : {}),
});

function segmentCount(): number {
	return readdirSync(join(dbPath, 'transaction_logs', log)).filter((name) =>
		name.endsWith('.txnlog')
	).length;
}

try {
	if (mode === 'write-frames') {
		const txnLog = db.useLog(log);
		for (let i = 0; i < 14; i++) {
			await db.transaction(async (txn) => {
				txn.setTimestamp(i === 0 ? key! - 60 * 60 * 1000 : key!);
				await txn.put(`k${i}`, 'v');
				txnLog.addEntry(Buffer.alloc(FRAME_PAYLOAD, i), txn.id);
			});
		}
		console.log(JSON.stringify({ wrote: key, entries: 14, payload: FRAME_PAYLOAD }));
	} else if (mode === 'write' || mode === 'write-unseeded' || rotating) {
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
		const clock = db.getMonotonicTimestamp();
		const now = Date.now();
		console.log(JSON.stringify({ clock, key, now }));
		if (clock > key!) {
			fail(`clock ${clock} was seeded from a log that was not named as locally originated`);
		}
		if (!(clock >= now - 60000 && clock <= now + 60000)) {
			fail(`clock ${clock} is not tracking the wall clock ${now}`);
		}
	} else if (mode === 'reopen-warn') {
		const second = RocksDatabase.open(dbPath, { name: 'other', timestampFloorLog: log });
		const until = Date.now() + 5000;
		while (Date.now() < until && warnings.length === 0) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		second.close();
		console.log(JSON.stringify({ warnings, clock: db.getMonotonicTimestamp(), key }));
		if (!warnings.some((warning) => warning.includes('was ignored'))) {
			fail('a second open with timestampFloorLog did not warn');
		}
	} else if (mode === 'warn' || mode === 'warn-read-only') {
		const until = Date.now() + 5000;
		while (
			Date.now() < until &&
			!warnings.some((warning) => (expectedWarning ? warning.includes(expectedWarning) : true))
		) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ warnings, clock, key }));
		if (!warnings.some((warning) => (expectedWarning ? warning.includes(expectedWarning) : true))) {
			fail('no clock-floor warning was emitted');
		}
	} else {
		fail(`unknown mode ${mode}`);
	}
} finally {
	db.close();
}
