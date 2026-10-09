import { RocksDatabase } from '../../src/index.ts';
import { writeSync } from 'node:fs';

if (process.argv.length < 3) {
	throw new Error('Missing database path');
}

const db = RocksDatabase.open(process.argv[2]);
const log = db.useLog('foo');
for (const text of ['after-purge-1', 'after-purge-2']) {
	await db.transaction(async (txn) => {
		log.addEntry(Buffer.from(text), txn.id);
		txn.putSync(text, text);
	});
}

// Synchronous: a queued async write would be lost to the SIGKILL on the next line.
writeSync(1, 'ready\n');
process.kill(process.pid, 'SIGKILL');
