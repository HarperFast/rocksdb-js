import { RocksDatabase } from '../../src/index.ts';

// Runs in its own process on purpose: the process that opened the database
// retains its `paths` list as the destroy layout, and a writable open that does
// not extend that list is refused before RocksDB ever sees it. `db_paths` is
// written nowhere on disk, so a FRESH process has no such record — it hands the
// list straight to RocksDB, which reports the MANIFEST as corrupt. That is the
// failure `explainOpenFailure` exists to explain, and only a new process can
// reach it.

const [dbPath, fast, slow] = process.argv.slice(2);

function expectGuidance(label: string, open: () => RocksDatabase): void {
	try {
		open().close();
	} catch (error) {
		const message = (error as Error)?.message ?? '';
		if (!/opened with `paths`, that same list/.test(message)) {
			console.error(`${label}: unexpected message: ${message}`);
			process.exit(1);
		}
		return;
	}
	console.error(`${label}: expected the open to fail`);
	process.exit(1);
}

// Nothing on disk records which list the MANIFEST's path indexes were written
// against, so neither of these is detectable up front the way the zero-to-one
// transition is. Both are recoverable by putting the list back.
expectGuidance('removed', () => RocksDatabase.open(dbPath));
expectGuidance('reordered', () =>
	RocksDatabase.open(dbPath, {
		paths: [
			{ path: slow, targetSize: 0 },
			{ path: fast, targetSize: 1 << 30 },
		],
	})
);

const db = RocksDatabase.open(dbPath, {
	paths: [
		{ path: fast, targetSize: 0 },
		{ path: slow, targetSize: 1 << 30 },
	],
});
if (db.getSync('key-0') !== 'value-0') {
	console.error(`recovered: expected value-0, got ${db.getSync('key-0')}`);
	process.exit(1);
}
db.close();
