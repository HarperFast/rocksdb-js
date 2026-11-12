// Run with ROCKSDB_JS_SYNC_WAL_FAILURE=2: the first two WAL syncs fail with an injected I/O error.
import { RocksDatabase } from '../../src/index.ts';

const db = RocksDatabase.open(process.argv[2]);
db.putSync('key', 'value');
try {
	db.syncWalSync();
	throw new Error('Expected syncWalSync() to throw the injected failure');
} catch (error) {
	if (!String(error).includes('Injected WAL sync failure')) throw error;
}
try {
	await db.syncWal();
	throw new Error('Expected syncWal() to reject with the injected failure');
} catch (error) {
	if (!String(error).includes('Injected WAL sync failure')) throw error;
}
db.syncWalSync();
db.close();
