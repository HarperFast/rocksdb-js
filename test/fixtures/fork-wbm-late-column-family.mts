import { RocksDatabase } from '../../src/index.ts';
import { join } from 'node:path';

// Usage: fork-wbm-late-column-family.mts <dbPath>
//
// A database keeps whichever WriteBufferManager it was opened with — `write_buffer_manager` is an
// immutable `DBOptions` field — while `writeBufferManagerSize` is mutable, and dropping it to 0 is
// documented as "no new attachments" rather than a teardown. So a column family created after that
// change still has its history charged to the manager the database is holding, and must still get
// the clamped target. Reading the global setting instead would hand it the derived 256MB.
//
// Its own process because the manager is a native process-global built on first open.
const [dbPath] = process.argv.slice(2);

if (!dbPath) {
	console.error('Usage: fork-wbm-late-column-family.mts <dbPath>');
	process.exit(1);
}

RocksDatabase.config({ writeBufferManagerSize: 128 * 1024 * 1024 });

const db = RocksDatabase.open(join(dbPath, 'db'));

RocksDatabase.config({ writeBufferManagerSize: 0 });

const late = RocksDatabase.open(join(dbPath, 'db'), { name: 'late' });
late.putSync(Buffer.from('k'), Buffer.alloc(1024, 1));

// An explicit positive target is still the caller's to choose on a late family, clamp or no clamp.
const explicitTarget = RocksDatabase.open(join(dbPath, 'db'), {
	name: 'explicit',
	maxWriteBufferSizeToMaintain: 256 * 1024 * 1024,
});
explicitTarget.putSync(Buffer.from('k'), Buffer.alloc(1024, 1));

explicitTarget.close();
late.close();
db.close();
