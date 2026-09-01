import { RocksDatabase } from '../../src/index.ts';
import type { Transaction } from '../../src/index.ts';
/**
 * Commit-stamping assertions driven through the ASYNC commit path, run by
 * test/commit-stamping-async.test.ts under each commit execution mode
 * (ROCKSDB_JS_COMMIT_THREAD unset = single lane, '0' = legacy libuv,
 * '2' = two-lane pipeline — the mode is read once per process, hence a child
 * per mode). Exits non-zero with a message on any violated invariant.
 *
 * Covered per mode: keep path (async), re-stamp path (async, caller timestamp
 * below watermark → fresh receiver stamp in record word + batch key +
 * committedLocalTime), mixed stamped/unstamped CFs in one re-stamped commit,
 * and the #668 pinned retry (log batch durable, IsBusy, auto-retry commits at
 * the pinned stamp).
 */
import { strict as assert } from 'node:assert';

const [dbPath] = process.argv.slice(2);

function stampedValue(payload: string): Buffer {
	const value = Buffer.alloc(8 + payload.length);
	value.write(payload, 8);
	return value;
}

const db = RocksDatabase.open(dbPath, {
	commitStamping: true,
	encoding: 'binary',
	transactionLogsPath: `${dbPath}-logs`,
});
const plain = RocksDatabase.open(dbPath, { encoding: 'binary', name: 'plain' });
const log = db.useLog('audit');

// Keep path.
const keep = (await db.transaction((t: Transaction): Transaction => {
	t.putSync('keep', stampedValue('keep'));
	log.addEntry(Buffer.from('keep-entry'), t.id);
	return t;
})) as Transaction;
const keepStamp = keep.getCommittedLocalTime();
assert.equal(keepStamp, keep.getTimestamp(), 'keep path must keep the txn timestamp');
assert.equal(
	(db.getSync('keep') as Buffer).readDoubleBE(0),
	keepStamp,
	'keep record word must equal the stamp'
);

// Re-stamp path with a mixed unstamped CF.
const rawWord = 77.25;
const restamp = (await db.transaction((t: Transaction): Transaction => {
	t.setTimestamp(2000.5);
	t.putSync('restamp', stampedValue('restamp'));
	plain.putSync('plain', stampedValue('plain'), { transaction: t });
	log.addEntry(Buffer.from('restamp-entry'), t.id);
	return t;
})) as Transaction;
const restampStamp = restamp.getCommittedLocalTime()!;
assert.notEqual(restampStamp, 2000.5, 're-stamp must not keep the stale caller timestamp');
assert.ok(restampStamp > keepStamp!, 're-stamp must exceed the watermark');
assert.equal(
	(db.getSync('restamp') as Buffer).readDoubleBE(0),
	restampStamp,
	're-stamped record word must equal the finalized stamp'
);
{
	const plainValue = plain.getSync('plain') as Buffer;
	assert.equal(plainValue.readDoubleBE(0), 0, 'unstamped CF bytes must be untouched');
	assert.equal(plainValue.subarray(8).toString(), 'plain');
}

// Pinned retry (#668): the log batch is written on attempt 1, the RocksDB
// commit loses to a conflicting direct write, and the auto-retried commit must
// re-apply the exact stamp the durable batch is keyed by.
let attempts = 0;
const retried = (await db.transaction((t: Transaction): Transaction => {
	attempts++;
	t.putSync('contested', stampedValue(`attempt-${attempts}`));
	log.addEntry(Buffer.from(`contested-entry-${attempts}`), t.id);
	if (attempts === 1) {
		// A conflicting committed write AFTER this txn staged its put.
		db.putSync('contested', stampedValue('interloper'));
	}
	return t;
})) as Transaction;
assert.ok(attempts >= 2, `expected an IsBusy retry (attempts=${attempts})`);
const pinnedStamp = retried.getCommittedLocalTime()!;
assert.equal(
	(db.getSync('contested') as Buffer).readDoubleBE(0),
	pinnedStamp,
	'retried record word must equal the pinned stamp'
);

// Log keys: every batch key must equal its commit's stamp (stamp-as-key), and
// the contested batch appears once, keyed at the pinned stamp.
const entries = [...log.query({ start: 1 })].map((entry) => ({
	timestamp: entry.timestamp,
	data: Buffer.from(entry.data).toString(),
}));
const byData = new Map(entries.map((entry) => [entry.data, entry.timestamp]));
assert.equal(byData.get('keep-entry'), keepStamp, 'keep batch key');
assert.equal(byData.get('restamp-entry'), restampStamp, 're-stamp batch key');
assert.equal(byData.get('contested-entry-1'), pinnedStamp, 'pinned batch key');
assert.equal(
	entries.filter((entry) => entry.data.startsWith('contested-entry')).length,
	1,
	'the WAL batch must be write-once across the retry (#668)'
);

plain.close();
db.close();
process.exit(0);
