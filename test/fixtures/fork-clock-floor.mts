// One mode per process, so a reader really starts from a fresh process-global
// clock. Prints one JSON line; exits 0 on success.
import { RocksDatabase } from '../../src/index.ts';
import { mkdirSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [mode, dbPath, keyArg] = process.argv.slice(2);
const key = keyArg ? Number(keyArg) : undefined;
const LOG = 'clock';

async function writeKeyed(db: RocksDatabase, timestamp: number, payload: Buffer) {
	await db.transaction(async (txn) => {
		txn.setTimestamp(timestamp);
		await txn.put('k', 'v');
		db.useLog(LOG).addEntry(payload, txn.id);
	});
}

function segments(): string[] {
	return readdirSync(join(dbPath, 'transaction_logs', LOG)).filter((f) => f.endsWith('.txnlog'));
}

const warnings: string[] = [];
const onWarn = (...args: unknown[]) => warnings.push(JSON.stringify(args));
if (mode === 'far-read') {
	RocksDatabase.on('log.warn', onWarn);
}
if (mode === 'warn') {
	// A short, non-empty file at a higher sequence than the real segment: its
	// header cannot be read, so the seed is incomplete and open must say so.
	mkdirSync(join(dbPath, 'transaction_logs', LOG), { recursive: true });
	writeFileSync(join(dbPath, 'transaction_logs', LOG, '9.txnlog'), 'FOOW');
	RocksDatabase.on('log.warn', onWarn);
}

const db = RocksDatabase.open(
	dbPath,
	mode === 'write-rotated' ? { transactionLogMaxSize: 64 * 1024 } : {}
);
try {
	if (mode === 'write') {
		await writeKeyed(db, key!, Buffer.from('entry'));
		console.log(JSON.stringify({ wrote: key, segments: segments().length }));
	} else if (mode === 'write-rotated') {
		// `key` lands in segment 1 (a batch may exceed the segment size when the
		// segment is empty); the lower-keyed batch does not fit and rotates, so
		// the reader can only learn `key` from segment 2's header word.
		await writeKeyed(db, key!, Buffer.alloc(100 * 1024, 1));
		await writeKeyed(db, key! - 3600 * 1000, Buffer.alloc(100 * 1024, 2));
		const count = segments().length;
		console.log(JSON.stringify({ wrote: key, segments: count }));
		if (count < 2) {
			process.exit(1);
		}
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
	} else if (mode === 'warn') {
		// The warning is delivered to the JS thread asynchronously.
		const until = Date.now() + 5000;
		while (Date.now() < until && !warnings.some((w) => w.includes('clock floor'))) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		RocksDatabase.off('log.warn', onWarn);
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ warnings: warnings.length, clock, key }));
		if (!warnings.some((w) => w.includes('clock floor')) || !(clock > key!)) {
			process.exit(1);
		}
	} else if (mode === 'far-write') {
		await writeKeyed(db, 8.64e15 - 1, Buffer.from('entry'));
		console.log(JSON.stringify({ wrote: 8.64e15 - 1 }));
	} else if (mode === 'far-read') {
		// The durable key is centuries ahead: it must be refused as a seed, with
		// a warning, rather than move the process clock there.
		const until = Date.now() + 5000;
		while (Date.now() < until && !warnings.some((w) => w.includes('ahead of the wall clock'))) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		RocksDatabase.off('log.warn', onWarn);
		const now = Date.now();
		const clock = db.getMonotonicTimestamp();
		console.log(JSON.stringify({ warnings: warnings.length, clock, now }));
		if (!warnings.some((w) => w.includes('ahead of the wall clock')) || !(clock < now + 1000)) {
			process.exit(1);
		}
	} else {
		console.error(`unknown mode ${mode}`);
		process.exit(2);
	}
} finally {
	db.close();
}
