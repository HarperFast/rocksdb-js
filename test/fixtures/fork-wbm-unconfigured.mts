// The manager is a process-wide singleton shared by every worker thread in the
// process, so a vitest worker can only observe the "never configured" shape if no
// other test file in that process has configured one — which nothing guarantees.
import { getWriteBufferManagerStats, RocksDatabase } from '../../src/index.ts';

const dbPath = process.argv[2];
if (!dbPath) {
	process.exit(1);
}

const db = RocksDatabase.open(dbPath);
db.putSync('foo', 'bar');
const stats = getWriteBufferManagerStats();
const dbStats = db.getStats();
db.close();

console.log(JSON.stringify({ stats, dbStats }));
