import { getRegistryStatus, RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// A dying worker's env-cleanup hook snapshots every registered descriptor. Run with
// ROCKSDB_JS_ENV_CLEANUP_PIN_DELAY_MS so the main thread's last-handle close lands while the
// snapshot pins the descriptor: that close skips its purge, and the hook must retry it.
const path = process.argv[2];
const db = RocksDatabase.open(path, { disableWAL: true });
db.putSync('key', 'value');

const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/env-cleanup-pin-worker.mts'),
	{
		eval: true,
		workerData: { path },
	}
);
const exited = new Promise<number>((resolve, reject) => {
	worker.once('exit', resolve);
	worker.once('error', reject);
});
await new Promise<void>((resolve, reject) => {
	worker.once('message', () => resolve());
	worker.once('error', reject);
});
// With the worker's handle closed, only the registry and `db` reference the descriptor until the
// worker's cleanup hook snapshots it.
const deadline = Date.now() + 10_000;
while ((getRegistryStatus().find((entry) => entry.path === path)?.refCount ?? 0) <= 2) {
	if (Date.now() > deadline) throw new Error('The worker cleanup hook never pinned the descriptor');
	await delay(5);
}
db.close();
// The hook releases its pin between steps; a close that landed in that gap purged itself.
if (!getRegistryStatus().some((entry) => entry.path === path)) {
	throw new Error('The close did not overlap the cleanup hook pin');
}
const code = await exited;
if (code !== 0) throw new Error(`Worker exited with code ${code}`);

const leaked = getRegistryStatus().filter((entry) => entry.path === path);
if (leaked.length > 0) {
	throw new Error(
		`Descriptor stayed registered after its last handle closed: ${JSON.stringify(leaked)}`
	);
}
