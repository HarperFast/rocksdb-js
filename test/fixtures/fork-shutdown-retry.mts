import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
try {
	db.close();
	throw new Error('Expected the initial close to fail');
} catch (error) {
	if (!String(error).includes('Injected database close failure')) throw error;
}

const worker = new Worker(createWorkerBootstrapScript('./test/workers/shutdown-retry-worker.mts'), {
	eval: true,
});
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}
await nextMessage(); // worker started, about to call shutdown()
const shutdownResult = nextMessage();

// The worker posts before calling shutdown(), so wait for the retry to actually
// be claimed. Opening earlier hits the still-quarantined entry and fails with
// "previous close failed" instead of exercising the wait this fixture measures.
function retrying(): boolean {
	return registryStatus().some((entry) => entry.path === path && entry.closeRetrying);
}
for (let attempt = 0; attempt < 40 && !retrying(); attempt++) await delay(25);
if (!retrying()) throw new Error('The shutdown retry was never claimed');

const started = Date.now();
// Timing only. shutdown() is process-wide and its loop can still be re-scanning
// after this open() returns but before the worker posts `shutdownResult`, so a
// handle opened here may be force-closed; read data from a fresh handle below.
RocksDatabase.open(path);
const elapsed = Date.now() - started;
if (elapsed < 500) throw new Error(`Open did not wait for the shutdown retry (${elapsed}ms)`);
const result = await shutdownResult;
if (!result.shutdown) throw new Error(`Shutdown retry failed: ${JSON.stringify(result)}`);
await worker.terminate();

const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value') throw new Error('Shutdown retry did not preserve data');
reopened.destroy();
