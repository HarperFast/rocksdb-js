import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-wbm-stall-watchdog.mts <dbPath>');
	process.exit(1);
}

const COLUMN_FAMILIES = 4;
// Explicit, and far above the budget: the derived default already resolves to 0
// under a stalling manager, so only an explicit retention target fills the budget
// with history nothing can release.
const MAINTAIN = 32 * 1024 * 1024;
const BUDGET = 4 * 1024 * 1024;

RocksDatabase.config({
	blockCacheSize: 8 * 1024 * 1024,
	writeBufferManagerSize: BUDGET,
	writeBufferManagerAllowStall: true,
});

// This thread never writes: it is the observer, and a write here would park it in
// the same stall it is trying to report on.
const db = RocksDatabase.open(dbPath, { maxWriteBufferSizeToMaintain: MAINTAIN });

// The programmatic half of the warn line. It reaches this thread through a
// threadsafe function, so it is only delivered because nothing here is blocked.
RocksDatabase.on('log.warn', (message: string) => {
	console.log(`WARNED ${message}`);
});

const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/wbm-stall-writer-worker.mts'),
	{
		eval: true,
		workerData: { path: dbPath, columnFamilies: COLUMN_FAMILIES, maintain: MAINTAIN },
	}
);
worker.unref();
await new Promise<void>((resolve, reject) => {
	worker.once('message', () => resolve());
	worker.once('error', reject);
});

// Poll until the stall has been active long enough for the watchdog to have
// reported, plus several more of its samples to prove it does not repeat.
const OBSERVE_AFTER_REPORT_MS = 6000;
const deadline = Date.now() + 90_000;
let sawStall = false;
let stalledSince = 0;
while (Date.now() < deadline) {
	const stats = RocksDatabase.getWriteBufferManagerStats();
	const fromGetStats = db.getStats();
	console.log(
		`STATS ${JSON.stringify({
			stats,
			getStats: {
				bufferSize: fromGetStats['writeBufferManager.bufferSize'],
				memoryUsage: fromGetStats['writeBufferManager.memoryUsage'],
				mutableMemoryUsage: fromGetStats['writeBufferManager.mutableMemoryUsage'],
				stallActive: fromGetStats['writeBufferManager.stallActive'],
				stallActiveMs: fromGetStats['writeBufferManager.stallActiveMs'],
			},
			getStat: {
				bufferSize: db.getStat('writeBufferManager.bufferSize'),
				stallActive: db.getStat('writeBufferManager.stallActive'),
				stallActiveMs: db.getStat('writeBufferManager.stallActiveMs'),
			},
		})}`
	);
	if (stats.stallActive) {
		if (!sawStall) {
			sawStall = true;
			stalledSince = Date.now();
		}
	} else if (sawStall) {
		// The stall cleared before the observation window closed; this run cannot
		// prove the one-line property, so say so rather than assert on it.
		console.log('CLEARED');
		break;
	}
	if (sawStall && Date.now() - stalledSince > OBSERVE_AFTER_REPORT_MS) {
		break;
	}
	await delay(250);
}

console.log(sawStall ? 'STALLED' : 'NEVER_STALLED');

// The parent owns the deadline and kills this process. It cannot exit on its own:
// the writer thread is parked inside RocksDB, so teardown would wedge in close()
// waiting on the same stall (see AGENTS.md note 16) — which is precisely why a
// test that reaches a real stall has to be driven from a killable child.
setInterval(() => {}, 1000);
