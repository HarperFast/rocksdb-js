/**
 * Regression fixture for the writeStall debounce blocker: the per-CF debounce
 * FSM must advance even while no one is listening, otherwise a CF marked stalled
 * while a listener was attached stays stuck `stalledReported` after the listener
 * detaches and the CF recovers unobserved — permanently suppressing the rising
 * edge for any listener attached later.
 *
 * Run with ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS=0 so recovery clears the FSM
 * immediately (no window), making the outcome independent of flush timing:
 *   1. attach a listener, provoke a stall (rising edge observed),
 *   2. detach it, stop writing so the CFs recover with no listener attached,
 *   3. re-attach and provoke a stall again — a fixed implementation re-emits.
 */
import { RocksDatabase, shutdown } from '../../src/index.ts';
import { rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';

const dbPath = process.argv[2];
const CF_COUNT = 8;
const opts = { writeBufferSize: 64 * 1024, maxWriteBufferNumber: 2, dbWriteBufferSize: 256 * 1024 };
const dbs: RocksDatabase[] = [];
for (let i = 0; i < CF_COUNT; i++) {
	const db = new RocksDatabase(dbPath, i === 0 ? opts : { ...opts, name: `cf-${i}` });
	db.open();
	dbs.push(db);
}

const value = Buffer.alloc(4096, 0x61);
let record = 0;
async function driveUntilStall(events: unknown[], maxMs: number): Promise<boolean> {
	const start = performance.now();
	while (performance.now() - start < maxMs) {
		for (let i = 0; i < 50; i++, record++) {
			dbs[record % CF_COUNT].putSync(`k-${record.toString().padStart(8, '0')}`, value);
		}
		await delay(0);
		if (events.length > 0) return true;
	}
	return false;
}

// Phase 1: listener attached, provoke a stall.
const e1: { cf: string; prev: string; cur: string }[] = [];
const l1 = (...a: any[]) => void e1.push({ cf: a[0], prev: a[1], cur: a[2] });
dbs[0].addListener('writeStall', l1);
const firstStall = await driveUntilStall(e1, 5000);

// Phase 2: detach, then stop writing so the CFs recover with nothing listening.
dbs[0].removeListener('writeStall', l1);
await delay(1000);

// Phase 3: re-attach and provoke a stall again.
const e2: { cf: string; prev: string; cur: string }[] = [];
const l2 = (...a: any[]) => void e2.push({ cf: a[0], prev: a[1], cur: a[2] });
dbs[0].addListener('writeStall', l2);
const secondStall = await driveUntilStall(e2, 5000);

console.log('RESULT ' + JSON.stringify({ firstStall, secondStall, e2sample: e2.slice(0, 3) }));

for (let i = dbs.length - 1; i >= 0; i--) {
	dbs[i].close();
}
shutdown();
rmSync(dbPath, { force: true, recursive: true, maxRetries: 3 });
