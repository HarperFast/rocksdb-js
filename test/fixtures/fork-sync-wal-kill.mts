// Writes without flushing, syncs the WAL synchronously, then SIGKILLs itself so no close or
// flush runs: the parent reopens and reads the writes back from the WAL. The kernel keeps the page
// cache across the kill, so this does not prove the sync reached the disk.
import { RocksDatabase } from '../../src/index.ts';
import { writeSync } from 'node:fs';

const db = RocksDatabase.open(process.argv[2]);
for (let i = 0; i < 100; i++) {
	db.putSync(`key-${i}`, { i });
}
db.syncWalSync();
writeSync(1, 'ready\n');
process.kill(process.pid, 'SIGKILL');
