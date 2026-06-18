import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

// Opens the SAME database path as its sibling workers (passed via workerData),
// writes a burst of records, then exits. Half the workers close explicitly and
// half exit with the database still open, so the env-cleanup hook tears down the
// shared, process-global DBDescriptor concurrently across worker envs — the race
// that used to corrupt the heap in DBRegistry::CloseDB.
const db = RocksDatabase.open(workerData.path, { disableWAL: true });
const payload = Buffer.alloc(64, 'x');

for (let i = 0; i < workerData.puts; i++) {
	db.putSync(`key-${(Math.random() * 5000) | 0}`, payload);
}

if (workerData.close) {
	db.close();
}

parentPort?.postMessage({ done: true });
process.exit(0);
