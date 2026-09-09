// In a child so `ROCKSDB_JS_WBM_STALL_WARN_MS` can be set: it is read once per process.
import { getWriteBufferManagerStats, RocksDatabase } from '../../src/index.ts';

const dbPath = process.argv[2];
if (!dbPath) {
	process.exit(1);
}

RocksDatabase.config({
	writeBufferManagerSize: 64 * 1024 * 1024,
	writeBufferManagerAllowStall: true,
});
const db = RocksDatabase.open(dbPath);
db.putSync('foo', 'bar');
const stats = getWriteBufferManagerStats();
db.close();

console.log(JSON.stringify(stats));
