import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const db = RocksDatabase.open(path);
for (let i = 0; i < 60; i++) {
	const column = RocksDatabase.open(path, { name: `registry-status-close-race-${i}` });
	column.close();
}

const worker = new Worker(
	createWorkerBootstrapScript('./test/workers/registry-status-worker.mts'),
	{ eval: true }
);
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready)
	throw new Error(`Registry-status worker failed to initialize: ${JSON.stringify(ready)}`);
worker.postMessage({ run: true });
const started = await nextMessage();
if (!started.started)
	throw new Error(`Unexpected registry-status result: ${JSON.stringify(started)}`);
const finished = nextMessage();
await delay(100);
db.close();
const result = await finished;
if (result.error) throw new Error(`Registry-status worker failed: ${result.error}`);
if (!result.finished)
	throw new Error(`Unexpected registry-status result: ${JSON.stringify(result)}`);
await worker.terminate();

if (registryStatus().some((entry) => entry.path === path)) {
	throw new Error('registryStatus() retained the descriptor after the last handle closed');
}
