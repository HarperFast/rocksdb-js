import { RocksDatabase } from '../../src/index.ts';
import { join } from 'node:path';

// Usage: fork-wbm-unconfigured-late-cf.mts <dbPath>
//
// #823 follow-up: the late-column-family path tested "is a WriteBufferManager attached" by reading
// RocksDB's *sanitized* DBOptions, which is never null post-open — RocksDB fills a missing manager
// with a disabled `WriteBufferManager(0)` — so a family created after the first open was clamped to
// 1 byte of retained history even with no manager ever configured for this process. Its own process
// because the manager is a native process-wide singleton and this scenario needs one nothing else
// has built (never calling `config()` at all).
const [dbPath] = process.argv.slice(2);

if (!dbPath) {
	console.error('Usage: fork-wbm-unconfigured-late-cf.mts <dbPath>');
	process.exit(1);
}

const db = RocksDatabase.open(join(dbPath, 'db'));

const late = RocksDatabase.open(join(dbPath, 'db'), { name: 'late' });
late.putSync(Buffer.from('k'), Buffer.alloc(1024, 1));

late.close();
db.close();
