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
await delay(100);
db.close();
const code = await exited;
if (code !== 0) throw new Error(`Worker exited with code ${code}`);

const leaked = getRegistryStatus().filter((entry) => entry.path === path);
if (leaked.length > 0) {
	throw new Error(
		`Descriptor stayed registered after its last handle closed: ${JSON.stringify(leaked)}`
	);
}
