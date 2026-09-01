/**
 * Child process for the commit-stamping fault matrix
 * (docs/design/local-mutation-stamping.md §5). Opens a stamping-enabled
 * database with a transaction log, commits a BASELINE record + log entry
 * (proving a transaction boundary in the active segment), then attempts the
 * CRASH commit — the parent arms ROCKSDB_JS_CRASH_POINT so this process dies
 * (exit 137) at the boundary under test. The parent reopens and asserts the
 * matrix row.
 *
 * argv: <dbPath> <mode>
 *   baseline     — commit the baseline, print its stamp, clean close, exit 0.
 *                  Run UNARMED (the seams would fire on this commit too).
 *   crash        — reopen (the durable marker inherits stamping) and attempt
 *                  the crash commit (single entry); run ARMED.
 *   crash-large  — same with a batch exceeding IOV_MAX entries (drives the
 *                  mid-log-append torn write).
 * stdout (baseline mode): JSON { baselineStamp }.
 */
import { RocksDatabase } from '../../src/index.ts';
import type { Transaction } from '../../src/index.ts';

const [dbPath, mode] = process.argv.slice(2);

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
const log = db.useLog('audit');

if (mode === 'baseline') {
	const baseline = (await db.transaction((t: Transaction): Transaction => {
		t.putSync('baseline', stampedValue('baseline'));
		log.addEntry(Buffer.from('baseline-entry'), t.id);
		return t;
	})) as Transaction;
	await new Promise<void>((resolve, reject) =>
		process.stdout.write(
			`${JSON.stringify({ baselineStamp: baseline.getCommittedLocalTime() })}\n`,
			(err) => (err ? reject(err) : resolve())
		)
	);
	db.close();
	process.exit(0);
}

if (mode === 'reserve-crash') {
	// A kept caller candidate inside the skew bound but beyond the open-time
	// reserve window forces a durable ceiling extension mid-claim — the armed
	// seam dies after the extension is durable, before it publishes.
	await db.transaction((t: Transaction) => {
		t.setTimestamp(Date.now() + 600000);
		t.putSync('reserve', stampedValue('reserve'));
	});
	db.close();
	process.exit(0);
}

const entryCount = mode === 'crash-large' ? 1500 : 1;
await db.transaction((t: Transaction) => {
	t.putSync('crash', stampedValue('crash-record'));
	for (let i = 0; i < entryCount; i++) {
		log.addEntry(Buffer.from(`crash-entry-${i}`), t.id);
	}
});
// Only reached when no crash point fired (the control run).
db.close();
process.exit(0);
