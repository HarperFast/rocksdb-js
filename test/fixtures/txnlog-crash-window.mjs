// Writes transactions to a log, forces a RocksDB flush partway through (so txn.state
// records a flushed position in the middle of the log), writes more, then SIGKILLs
// itself so no shutdown/close ever runs. The parent asserts the post-flush window is
// still readable by a *committed* query after reopening. See HarperFast/harper#1949.
import { RocksDatabase } from '../../dist/index.mjs';
import { writeSync } from 'node:fs';

if (process.argv.length < 3) {
	throw new Error('Missing database path');
}

const dbPath = process.argv[2];
const beforeFlush = Number.parseInt(process.env.BEFORE_FLUSH || '3', 10);
const afterFlush = Number.parseInt(process.env.AFTER_FLUSH || '4', 10);

const db = RocksDatabase.open(dbPath);
const log = db.useLog('foo');

async function write(count, prefix) {
	const value = Buffer.alloc(24, prefix);
	for (let i = 0; i < count; i++) {
		await db.transaction(async (txn) => {
			txn.putSync(`${prefix}-${i}`, { i });
			log.addEntry(value, txn.id);
		});
	}
}

await write(beforeFlush, 'a');
// Flush the memtable: OnFlushCompleted writes txn.state at the position reached so far.
await db.flush();
await write(afterFlush, 'b');

// Synchronous write: stdout is a pipe here, so a queued async console.log would be lost
// to the SIGKILL on the next line and the parent would never see the handshake.
writeSync(1, 'ready\n');
process.kill(process.pid, 'SIGKILL');
