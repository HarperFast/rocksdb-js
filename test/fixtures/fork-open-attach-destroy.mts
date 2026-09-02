import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const owner = RocksDatabase.open(path);
owner.putSync('before-destroy', 'present');

const worker = new Worker(createWorkerBootstrapScript('./test/workers/open-attach-worker.mts'), {
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
if (!ready.ready) throw new Error(`Open worker failed to initialize: ${JSON.stringify(ready)}`);
worker.postMessage({ open: true });
const opening = await nextMessage();
if (!opening.opening) throw new Error(`Open worker did not start: ${JSON.stringify(opening)}`);

await delay(100);
const registryEntry = registryStatus().find((entry) => entry.path === path);
const closables = registryEntry?.closables;

owner.destroy();
const result = await nextMessage();
await worker.terminate();

if (closables !== 2) {
	throw new Error(
		`Expected both database handles to be attached before open returned, got ${closables}`
	);
}
if (result.error) {
	throw new Error(`Racing open failed: ${result.error}`);
}
if (result.openedBeforeReopen !== false) {
	throw new Error(`Destroy did not close the racing open: ${JSON.stringify(result)}`);
}
if (result.reopened !== true || result.value !== 'present') {
	throw new Error(`Foreign-closed handle did not reopen cleanly: ${JSON.stringify(result)}`);
}
