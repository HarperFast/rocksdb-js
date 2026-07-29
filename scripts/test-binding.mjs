import { RocksDatabase, versions } from '../dist/index.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Published bindings promise that the boolean shorthand selects zlib. Keep
// that packaging guarantee here rather than in the generic native suite,
// where a custom ROCKSDB_PATH build may legitimately omit zlib.
const tempDir = await mkdtemp(join(tmpdir(), 'rocksdb-js-binding-'));
const db = new RocksDatabase(join(tempDir, 'db'), { compression: true });
try {
	db.open();
} finally {
	db.close();
	await rm(tempDir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
}

console.log(
	`rocksdb-js v${versions['rocksdb-js']} (RocksDB v${versions.rocksdb}) loaded successfully with zlib compression!`
);
