import { getWriteBufferManagerStats, RocksDatabase } from '../../src/index.ts';
import { join } from 'node:path';

// Usage: fork-wbm-cross-db-stall.mts <rootDir> <default|oldest>
//
// Builds the state where a stalling WriteBufferManager's memory is held by databases nothing is
// writing to, then writes to a different database. Under `kFlushOldest` only the database being
// written flushes, so that writer stalls forever; the default policy for a stalling manager
// (`largestAcrossDBs`) flushes the idle databases instead.
//
// Each idle peer holds one large memtable that has already been flushed but is still retained as
// history, plus one small mutable memtable from a single later write. That write schedules a history
// trim, but only another write to that database would run it. Flushing the small memtable is what
// releases the large one (`MemTableListVersion::Remove` trims all but the newest flushed memtable),
// so the flush the peer is waiting for actually frees memory. Flushing a peer whose only charge is
// its newest flushed memtable would free nothing, under either policy.
//
// A child process because the failure is a hang on the JS thread (`putSync` stalls inside RocksDB),
// which only the parent's kill can bound.
const [root, policy] = process.argv.slice(2);
if (!root || (policy !== 'default' && policy !== 'oldest')) {
	console.error('Usage: fork-wbm-cross-db-stall.mts <rootDir> <default|oldest>');
	process.exit(1);
}

const MiB = 1024 * 1024;
const PEERS = 3;
const PEER_BYTES = 12 * MiB;
const SHRUNK_BUDGET = 16 * MiB;

RocksDatabase.config({
	writeBufferManagerSize: 256 * MiB,
	writeBufferManagerAllowStall: true,
	...(policy === 'oldest' ? { writeBufferManagerFlushPolicy: 'oldest' as const } : {}),
});

// writeBufferSize far above the peers' data so only the manager can trigger a flush.
const options = { writeBufferSize: 256 * MiB };
const writer = RocksDatabase.open(join(root, 'writer'), options);
const peers = Array.from({ length: PEERS }, (_, i) =>
	RocksDatabase.open(join(root, `peer${i}`), options)
);

const value = Buffer.alloc(4096, 7);
for (const peer of peers) {
	for (let i = 0; i < PEER_BYTES / value.length; i++) {
		peer.putSync(`k${i}`, value);
	}
	peer.flushSync();
	peer.putSync('tail', 'x');
}

RocksDatabase.config({ writeBufferManagerSize: SHRUNK_BUDGET });
const setup = getWriteBufferManagerStats();
console.log(
	`SETUP ${JSON.stringify({ memoryUsage: setup.memoryUsage, bufferSize: setup.bufferSize, flushPolicy: setup.flushPolicy })}`
);
if (setup.memoryUsage < setup.bufferSize) {
	console.error('setup did not put the manager over budget');
	process.exit(3);
}

for (let i = 0; i < 64; i++) {
	writer.putSync(`w${i}`, value);
}
console.log(`WROTE ${writer.getSync('w63')?.length}`);
console.log(`FINAL ${JSON.stringify({ memoryUsage: getWriteBufferManagerStats().memoryUsage })}`);

writer.close();
for (const peer of peers) peer.close();
