import { RocksDatabase, Transaction } from '../../src/index.ts';
import { disablePrelogConflictCheckForTesting } from '../../src/load-binding.ts';
import { join } from 'node:path';

// Run with ROCKSDB_JS_BACKUP_UNRESOLVED_WAIT_MS set low: a backup that captures a logged batch which
// neither commits nor aborts within the bound is refused rather than published.
const dbPath = process.argv[2];
RocksDatabase.config({ transactionLogAbortMarkers: true });
// a logged batch must conflict at commit; the check before the log write would refuse it first
disablePrelogConflictCheckForTesting(true);
const db = RocksDatabase.open(dbPath);
try {
	const log = db.useLog('foo');
	const pinned = new Transaction(db.store);
	log.addEntry(Buffer.from('pinned'), pinned.id);
	await pinned.put('a', 'pinned');
	await db.put('a', 'conflict');
	await pinned.commit().catch(() => {}); // logged, conflicted, still pending
	const started = performance.now();
	const refused = await db
		.backup(join(dbPath, '..', `${process.pid}-backup`), { transactionLogs: true })
		.then(
			() => false,
			(err) => /neither committed nor been aborted/.test(String(err?.message))
		);
	const elapsed = performance.now() - started;
	if (!refused || elapsed < 150) {
		throw new Error(
			`expected a refused backup after the wait, refused=${refused} elapsed=${elapsed}`
		);
	}
	pinned.abort();
} finally {
	db.close();
}
