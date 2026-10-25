import { RocksDatabase, Transaction } from '../../src/index.ts';

// Run with ROCKSDB_JS_ABORT_MARK_FAILURE=2: the abort's marker fails, and so does the retry the
// next append makes first, so that append is refused; the append after it retries successfully.
const dbPath = process.argv[2];
const fail = (message: string): never => {
	throw new Error(message);
};

RocksDatabase.config({ transactionLogAbortMarkers: true });
const db = RocksDatabase.open(dbPath);
try {
	const log = db.useLog('foo');
	const txn = new Transaction(db.store);
	log.addEntry(Buffer.from('lost'), txn.id);
	await txn.put('a', 'lost');
	await db.put('a', 'conflict');
	await txn.commit().then(
		() => fail('commit should conflict'),
		(err) => err.code === 'ERR_BUSY' || fail(`unexpected commit error ${err.code}`)
	);
	for (let i = 0; i < 2; i++) {
		try {
			txn.abort();
			fail('abort should report the unresolved cancellation');
		} catch (err) {
			if ((err as { code?: string }).code !== 'ERR_TRANSACTION_ABORT_UNRESOLVED') {
				throw err;
			}
		}
	}
	if (log.getStats().totals.abortMarkFailures !== 1) {
		fail('expected one marker failure');
	}

	const refused = db.transaction(async (next) => {
		log.addEntry(Buffer.from('refused'), next.id);
		await next.put('b', 'refused');
	});
	await refused.then(
		() => fail('append should be refused while the cancellation is unresolved'),
		(err) =>
			/refuses appends/.test(String(err?.message)) || fail(`unexpected error ${err?.message}`)
	);

	await db.transaction(async (next) => {
		log.addEntry(Buffer.from('kept'), next.id);
		await next.put('b', 'kept');
	});
	txn.abort(); // resolved now: no error
	const entries = Array.from(log.query({ start: 0 }), (entry) => entry.data.toString());
	if (JSON.stringify(entries) !== JSON.stringify(['kept'])) {
		fail(`readers saw ${JSON.stringify(entries)}`);
	}
	const totals = log.getStats().totals;
	if (totals.abortsMarked !== 1 || totals.abortMarkFailures !== 2) {
		fail(`unexpected totals ${JSON.stringify(totals)}`);
	}
} finally {
	db.close();
}
