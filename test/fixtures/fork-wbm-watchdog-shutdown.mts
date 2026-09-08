// In a child because creating the manager is irreversible for the process: its
// `costToCache` is fixed at construction, so doing this in the shared vitest
// worker would decide it for every test file that runs afterwards. The parent
// also owns the deadline — a watchdog join that deadlocks hangs this process,
// and only a killable child turns that into a test failure.
import { getWriteBufferManagerStats, RocksDatabase, shutdown } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];
if (!dbPath) {
	process.exit(1);
}

const workerPath = join(import.meta.dirname, '..', 'workers', 'wbm-shutdown-worker.mts');
const CONCURRENT_SHUTDOWNS = 4;
const ROUNDS = 4;

/** Runs `shutdown()` from several envs at once, all released from one gate. */
async function shutdownConcurrently(): Promise<void> {
	const gate = new SharedArrayBuffer(4);
	const released = new Int32Array(gate);
	let ready = 0;

	await new Promise<void>((resolve, reject) => {
		const exits: Promise<void>[] = [];
		for (let i = 0; i < CONCURRENT_SHUTDOWNS; i++) {
			const worker = new Worker(createWorkerBootstrapScript(workerPath), {
				eval: true,
				workerData: { gate },
			});
			exits.push(
				new Promise<void>((done, fail) => {
					worker.on('error', fail);
					worker.on('exit', (code) => (code === 0 ? done() : fail(new Error(`exit ${code}`))));
				})
			);
			worker.on('message', () => {
				if (++ready === CONCURRENT_SHUTDOWNS) {
					Atomics.store(released, 0, 1);
					Atomics.notify(released, 0);
				}
			});
		}
		Promise.all(exits).then(() => resolve(), reject);
	});
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

// Only one caller may own the retiring thread; the others must wait for it
// rather than clear the stop latch and strand the owner inside join(). The race
// is narrow, so run several rounds — a lost one hangs here until the parent's
// deadline rather than failing an assertion.
for (let round = 0; round < ROUNDS; round++) {
	await shutdownConcurrently();
	watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
	db.open();
	watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
}
db.close();

console.log(JSON.stringify(watchdogRunning));
