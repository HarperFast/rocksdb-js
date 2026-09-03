import { RocksDatabase } from '../../src/index.ts';
import { join } from 'node:path';

// Usage: fork-wbm-late-allow-stall.mts <dbPath> <late|upfront>
//
// The transition this covers is one-way per process — a WriteBufferManager is a native
// process-global built on first open, and only a false -> true `allowStall` change on a live one
// warns — so each arm needs a process nothing else has configured a manager in.
//
// `late` opens a database under a non-stalling manager and then turns the stall on, which is the
// case the safeguard cannot reach: `max_write_buffer_size_to_maintain` is fixed when a column
// family is created. `upfront` configures the stall before opening anything, which must stay quiet.
const [dbPath, arm] = process.argv.slice(2);

if (!dbPath || (arm !== 'late' && arm !== 'upfront')) {
	console.error('Usage: fork-wbm-late-allow-stall.mts <dbPath> <late|upfront>');
	process.exit(1);
}

const warnings: string[] = [];
RocksDatabase.on('log.warn', (message: string) => {
	if (message.includes('writeBufferManagerAllowStall was enabled')) warnings.push(message);
});

RocksDatabase.config({
	writeBufferManagerSize: 16 * 1024 * 1024,
	writeBufferManagerAllowStall: arm === 'upfront',
});

// Opens the manager as well as the database — `DBDescriptor::open` is where it is built.
const db = new RocksDatabase(join(dbPath, 'db'));
db.open();

RocksDatabase.config({ writeBufferManagerAllowStall: true });

await new Promise((resolve) => setTimeout(resolve, 100));
db.close();

console.log(`WARNINGS ${warnings.length}`);
