import { RocksDatabase } from '../../src/index.ts';
import { appendFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

const [mode, dbPath, keyArg, logArg, expectedWarning] = process.argv.slice(2);
const key = keyArg ? Number(keyArg) : undefined;
const log = logArg ?? 'local';

function fail(message: string): never {
	console.error(message);
	process.exit(1);
}

const warnings: string[] = [];
if (mode.startsWith('warn')) {
	RocksDatabase.on('log.warn', (...args: unknown[]) => warnings.push(JSON.stringify(args)));
}

const rotating = mode === 'write-rotate';
const FRAME_PAYLOAD = 32;
let db!: RocksDatabase;
const unseededFirstOpen =
	mode === 'write-unseeded' || mode === 'reopen-refuse' || mode === 'reopen-refuse-read-only';
if (mode !== 'refuse-then-unseeded') {
	db = RocksDatabase.open(dbPath, {
		...(unseededFirstOpen ? {} : { timestampFloorLog: log }),
		...(mode === 'warn-read-only' ? { readOnly: true } : {}),
		...(rotating ? { transactionLogMaxSize: 64 * 1024 } : {}),
	});
}

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
	} else if (mode === 'reopen-refuse') {
		try {
			const second = RocksDatabase.open(dbPath, { name: 'other', timestampFloorLog: log });
			second.close();
			fail('a second open with timestampFloorLog unexpectedly succeeded');
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			console.log(JSON.stringify({ error: message, clock: db.getMonotonicTimestamp(), key }));
			if (!message.includes('monotonic timestamp floor was not seeded')) {
				fail(`unexpected second-open error: ${message}`);
			}
		}
	} else if (mode === 'refuse-then-unseeded') {
		try {
			RocksDatabase.open(dbPath, { timestampFloorLog: log });
			fail('an untrusted timestamp floor unexpectedly opened');
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			if (!message.includes('ahead of the wall clock')) {
				fail(`unexpected timestamp-floor error: ${message}`);
			}
		}
		const unseeded = RocksDatabase.open(dbPath);
		unseeded.close();
		console.log(JSON.stringify({ recovered: true }));
	} else if (mode === 'reopen-refuse-read-only') {
		// A different DBKey, so a different descriptor — but the same physical path,
		// and the clock it would be claiming was never seeded from this log.
		try {
			const second = RocksDatabase.open(dbPath, { readOnly: true, timestampFloorLog: log });
			second.close();
			fail('a read-only open with timestampFloorLog unexpectedly succeeded');
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			console.log(JSON.stringify({ error: message, clock: db.getMonotonicTimestamp(), key }));
			if (!message.includes('monotonic timestamp floor was not seeded')) {
				fail(`unexpected read-only open error: ${message}`);
			}
		}
	} else if (mode === 'reopen-same-log') {
		// Corruption a rescan would refuse on. The seed is a property of the path
		// and this one is already resolved, so the second open must not look again.
		const logDir = join(dbPath, 'transaction_logs', log);
		const segment = readdirSync(logDir).find((name) => name.endsWith('.txnlog'))!;
		appendFileSync(join(logDir, segment), Buffer.alloc(64, 7));
		const second = RocksDatabase.open(dbPath, { readOnly: true, timestampFloorLog: log });
		const clock = second.getMonotonicTimestamp();
		second.close();
		console.log(JSON.stringify({ clock, key }));
		if (!(clock > key!)) {
			fail(`clock ${clock} did not clear the durable key ${key}`);
		}
	} else if (mode === 'seed-then-write') {
		// No setTimestamp: the batch key comes from the seeded clock, so reopening
		// must find it in the log and seed above it.
		let written = 0;
		await db.transaction(async (txn) => {
			written = txn.getTimestamp();
			await txn.put('auto', 'v');
			db.useLog(log).addEntry(Buffer.from('auto'), txn.id);
		});
		console.log(JSON.stringify({ written, key }));
		if (!(written > key!)) {
			fail(`transaction timestamp ${written} did not clear the seeded key ${key}`);
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
	} else if (mode === 'warn-absent') {
		// Give any warning the same window the positive cases get before deciding
		// none arrived.
		await new Promise((resolve) => setTimeout(resolve, 250));
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ warnings, clock, key }));
		if (expectedWarning && warnings.some((warning) => warning.includes(expectedWarning))) {
			fail(`unexpected warning matching "${expectedWarning}": ${warnings.join(' ')}`);
		}
	} else {
		fail(`unknown mode ${mode}`);
	}
} finally {
	if (db) {
		db.close();
	}
}
