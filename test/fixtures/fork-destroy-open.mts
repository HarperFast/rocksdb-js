import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const original = RocksDatabase.open(path);
original.putSync('before-destroy', 'present');

const worker = new Worker(createWorkerBootstrapScript('./test/workers/destroy-open-worker.mts'), {
	eval: true,
	workerData: { path },
});

function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready) throw new Error(`Destroy worker failed to initialize: ${JSON.stringify(ready)}`);
worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Destroy worker did not start: ${JSON.stringify(destroying)}`);

const registryDeadline = Date.now() + 5_000;
while (registryStatus().some((entry) => entry.path === path)) {
	if (Date.now() >= registryDeadline) throw new Error('Timed out waiting for the destroy window');
	await delay(1);
}

const destroyResult = nextMessage();
const startedAt = Date.now();
const reopened = RocksDatabase.open(path);
const openDuration = Date.now() - startedAt;
const destroyed = await destroyResult;
if (!destroyed.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyed)}`);
if (openDuration < 500) throw new Error(`Reopen did not wait for destroy (${openDuration}ms)`);
if (reopened.getSync('before-destroy') !== undefined)
	throw new Error('Reopen observed pre-destroy data');
reopened.putSync('after-destroy', 'present');
if (reopened.getSync('after-destroy') !== 'present')
	throw new Error('Reopened database is not usable');
reopened.close();
await worker.terminate();
