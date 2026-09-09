// In a child because creating the manager pins its `costToCache` for the process,
// and because a join that deadlocks hangs this process — only a killable child
// turns that into a test failure rather than a wedged run.
import { getWriteBufferManagerStats, RocksDatabase, shutdown } from '../../src/index.ts';
import { setWriteBufferManagerJoinDelayForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

const dbPath = process.argv[2];
if (!dbPath) {
	process.exit(1);
}

// Cwd-relative, like the other worker fixtures: an absolute path here becomes a
// drive-letter import that Node's ESM loader rejects on Windows.
const workerPath = './test/workers/wbm-shutdown-worker.mts';
const CONCURRENT_SHUTDOWNS = 4;
const ROUNDS = 4;

async function shutdownConcurrently(): Promise<void> {
	await Promise.all(await startConcurrentShutdowns(CONCURRENT_SHUTDOWNS));
}

async function startConcurrentShutdowns(count: number): Promise<Promise<void>[]> {
	const gate = new SharedArrayBuffer(4);
	const released = new Int32Array(gate);
	let ready = 0;
	const exits: Promise<void>[] = [];

	await new Promise<void>((resolve, reject) => {
		for (let i = 0; i < count; i++) {
			const worker = new Worker(createWorkerBootstrapScript(workerPath), {
				eval: true,
				workerData: { gate },
			});
			worker.on('error', reject);
			exits.push(
				new Promise<void>((done, fail) => {
					worker.on('error', fail);
					worker.on('exit', (code) => (code === 0 ? done() : fail(new Error(`exit ${code}`))));
				})
			);
			worker.on('message', () => {
				if (++ready === count) {
					Atomics.store(released, 0, 1);
					Atomics.notify(released, 0);
					resolve();
				}
			});
		}
	});
	return exits;
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

setWriteBufferManagerJoinDelayForTesting(2, 5000);
try {
	const delayedShutdowns = await startConcurrentShutdowns(2);
	await Promise.race(delayedShutdowns);
	db.open();
	await Promise.all(delayedShutdowns);
	watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
} finally {
	setWriteBufferManagerJoinDelayForTesting(0, 0);
}

for (let round = 0; round < ROUNDS; round++) {
	await shutdownConcurrently();
	watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
	db.open();
	watchdogRunning.push(getWriteBufferManagerStats().watchdogRunning);
}
db.close();

console.log(JSON.stringify(watchdogRunning));
