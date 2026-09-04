import { RocksDatabase } from '../../src/index.ts';

// Runs in its own process on purpose: the blob cache lives in the native
// DBSettings singleton, which is shared across every Node env in the process
// (vitest's worker threads included), so "this process has no blob cache" is
// only reproducible somewhere no other suite has configured one.
//
// A cache-less process must still carry each column family's persisted
// prepopulate_blob_cache through a cold open. RocksDB rewrites the OPTIONS file
// on every open, so dropping the field here would let a CLI tool or maintenance
// script permanently turn the serving process's setting off.

const dbPath = process.argv[2];

function fail(message: string): never {
	console.error(message);
	process.exit(1);
}

RocksDatabase.config({ blobCacheSize: 0 });

let db = RocksDatabase.open(dbPath, { name: 't1', blobs: { prepopulateCache: true } });
db.putSync('key', 'x'.repeat(8192));
db.flushSync();
db.close();

// Cold-open a DIFFERENT family: t1 is reopened from its OPTIONS file alone.
const other = RocksDatabase.open(dbPath, { name: 't2' });

// The live column family is the oracle — the warm conflict check compares the
// request against it, so a restored kFlushOnly accepts `true` and rejects
// `false`. A dropped restore inverts both.
try {
	db = RocksDatabase.open(dbPath, { name: 't1', blobs: { prepopulateCache: true } });
	db.close();
} catch (error) {
	fail(`persisted prepopulateCache was not restored: ${(error as Error).message}`);
}

try {
	RocksDatabase.open(dbPath, { name: 't1', blobs: { prepopulateCache: false } });
	fail('expected a conflict for prepopulateCache false against a restored true');
} catch (error) {
	if (!/prepopulateCache/.test((error as Error).message)) {
		throw error;
	}
}

other.close();
