import { getWriteBufferManagerStats, RocksDatabase } from '../../src/index.ts';

// Usage: fork-wbm-flush-policy.mts <dbPath> <default|explicitFirst>
//
// A child process because the policy lives on the process-global WriteBufferManager. Prints one
// `POLICY <json>` line: the flush policy `getWriteBufferManagerStats()` reports after each step.
const [dbPath, mode] = process.argv.slice(2);
if (!dbPath || (mode !== 'default' && mode !== 'explicitFirst')) {
	console.error('Usage: fork-wbm-flush-policy.mts <dbPath> <default|explicitFirst>');
	process.exit(1);
}

const steps: Record<string, string> = {};
function record(step: string) {
	steps[step] = getWriteBufferManagerStats().flushPolicy;
}

if (mode === 'explicitFirst') {
	RocksDatabase.config({
		writeBufferManagerSize: 32 * 1024 * 1024,
		writeBufferManagerAllowStall: true,
		writeBufferManagerFlushPolicy: 'largest',
	});
	const db = RocksDatabase.open(dbPath);
	db.putSync('k', 'v');
	record('created');
	RocksDatabase.config({ writeBufferManagerAllowStall: false });
	record('stallOff');
	db.close();
	console.log(`POLICY ${JSON.stringify(steps)}`);
	process.exit(0);
}

record('unconfigured');
RocksDatabase.config({ writeBufferManagerAllowStall: true });
record('unconfiguredStallOn');
RocksDatabase.config({ writeBufferManagerAllowStall: false });

RocksDatabase.config({ writeBufferManagerSize: 32 * 1024 * 1024 });
const db = RocksDatabase.open(dbPath);
db.putSync('k', 'v');
record('created');

RocksDatabase.config({ writeBufferManagerAllowStall: true });
record('stallOn');
db.putSync('k', 'v2');

// A rejected policy must leave the rest of the call unapplied.
let rejected = '';
try {
	RocksDatabase.config({
		writeBufferManagerAllowStall: false,
		writeBufferManagerFlushPolicy: 'newest' as any,
	});
} catch (error) {
	rejected = (error as Error).name;
}
steps.rejected = rejected;
record('afterRejected');
steps.allowStallAfterRejected = String(getWriteBufferManagerStats().allowStall);

RocksDatabase.config({ writeBufferManagerAllowStall: false });
record('stallOff');

RocksDatabase.config({ writeBufferManagerFlushPolicy: 'largest' });
record('explicitLargest');
RocksDatabase.config({ writeBufferManagerAllowStall: true });
record('explicitLargestStallOn');
RocksDatabase.config({ writeBufferManagerFlushPolicy: 'oldest' });
record('explicitOldestStallOn');
RocksDatabase.config({
	writeBufferManagerAllowStall: false,
	writeBufferManagerFlushPolicy: 'largestAcrossDBs',
});
record('explicitAcrossStallOff');
RocksDatabase.config({ writeBufferManagerFlushPolicy: undefined });
record('undefinedKeepsExplicit');

db.putSync('k', 'v3');
steps.value = String(db.getSync('k'));
db.close();
console.log(`POLICY ${JSON.stringify(steps)}`);
