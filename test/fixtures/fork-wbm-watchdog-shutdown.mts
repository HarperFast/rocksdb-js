// In a child because creating the manager is irreversible for the process: its
// `costToCache` is fixed at construction, so doing this in the shared vitest
// worker would decide it for every test file that runs afterwards.
import { getWriteBufferManagerStats, RocksDatabase, shutdown } from '../../src/index.ts';

const dbPath = process.argv[2];
if (!dbPath) {
	process.exit(1);
}

RocksDatabase.config({
	writeBufferManagerSize: 64 * 1024 * 1024,
	writeBufferManagerAllowStall: true,
});

const db = new RocksDatabase(dbPath);
const watchdogRunning: boolean[] = [];
db.open();
watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
shutdown();
watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
db.open();
watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
db.close();

console.log(JSON.stringify(watchdogRunning));
