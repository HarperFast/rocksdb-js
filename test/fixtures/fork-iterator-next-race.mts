import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const original = RocksDatabase.open(path);
original.putSync('a', '1');
original.putSync('b', '2');

const worker = new Worker(createWorkerBootstrapScript('./test/workers/destroy-open-worker.mts'), {
	eval: true,
	workerData: { path, destroyStartDelayMs: 50 },
});

function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}

const ready = await nextMessage();
if (!ready.ready) throw new Error(`Worker failed to initialize: ${JSON.stringify(ready)}`);

const iterator = original.getRange({})[Symbol.iterator]();
const first = iterator.next();
if (first.done) throw new Error('Expected a first row before the destroy race');

worker.postMessage({ destroy: true });
const destroying = await nextMessage();
if (!destroying.destroying)
	throw new Error(`Worker did not start destroying: ${JSON.stringify(destroying)}`);

// Holds iteratorMutex for the ROCKSDB_JS_ITERATOR_NEXT_DELAY_MS test seam
// while the worker's destroy() -- ticking on its own 50ms delay -- reaches
// finishClose()'s closables sweep and blocks on the same mutex behind this
// call. This is the case iteratorMutex exists for: a foreign forced close
// racing an in-progress Next(), not just one racing the constructor.
const second = iterator.next();
if (second.done) throw new Error('Expected a second row before destroy claimed the iterator');

const destroyResult = await nextMessage();
if (!destroyResult.destroyed) throw new Error(`Destroy failed: ${JSON.stringify(destroyResult)}`);

if (registryStatus().some((entry) => entry.path === path)) {
	throw new Error('Expected destroy to fully clear the registry entry');
}

// The mutex handoff must leave the iterator cleanly (not torn/crashing)
// closed once finishClose() gets its turn.
try {
	iterator.next();
	throw new Error('Expected Next to fail once destroy closed the iterator');
} catch (error) {
	if (!String(error).includes('Iterator not initialized')) throw error;
}

await worker.terminate();
