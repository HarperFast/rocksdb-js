import { RocksDatabase } from '../../src/index.ts';

// Runs in its own process on purpose: "blobCacheSize has never been set
// explicitly" is a latched, process-global flag in the native DBSettings
// singleton — which is shared across every Node env in the process, vitest's
// worker threads included — so the derivation can only be observed by a process
// that has never configured one.

const dbPath = process.argv[2];

function check(label: string, actual: unknown, expected: unknown): void {
	if (actual !== expected) {
		console.error(`${label}: expected ${expected}, got ${actual}`);
		process.exit(1);
	}
}

// Never set explicitly, so a block cache size derives 10% of it for blobs.
RocksDatabase.config({ blockCacheSize: 10 * 1024 * 1024 });
let db = RocksDatabase.open(`${dbPath}-a`, { blobs: { prepopulateCache: true } });
check('derived', db.getDBIntProperty('rocksdb.blob-cache-capacity'), 1024 * 1024);
db.close();

// Once stated, a later call that says nothing about blobs must not discard it.
RocksDatabase.config({ blobCacheSize: 8 * 1024 * 1024 });
RocksDatabase.config({ blockCacheSize: 20 * 1024 * 1024 });
db = RocksDatabase.open(`${dbPath}-b`, { blobs: { prepopulateCache: true } });
check('latched', db.getDBIntProperty('rocksdb.blob-cache-capacity'), 8 * 1024 * 1024);
db.close();
