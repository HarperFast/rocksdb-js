import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

// Proves a `lifecycleWaitSeconds` timeout actually fires and the gate
// recovers afterward -- previously only config validation was tested.
// Single descriptor only: does not reproduce db_registry.cpp:585's
// multi-descriptor predicate/notifier mismatch.
RocksDatabase.config({ lifecycleWaitSeconds: 1 });

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

// Wait for the retry claim to actually land (closeRetrying: true) before
// racing the open below -- otherwise an open that wins the race hits the
// quarantine check first and throws "previous close failed" instead of
// timing out, which is a flake under load, not a proof.
for (let attempt = 0; attempt < 40; attempt++) {
	if (registryStatus().some((entry) => entry.path === path && entry.closeRetrying)) break;
	await delay(25);
}
if (!registryStatus().some((entry) => entry.path === path && entry.closeRetrying)) {
	throw new Error('Shutdown retry was never claimed');
}

const start = Date.now();
try {
	RocksDatabase.open(path);
	throw new Error('Expected open to time out while the retry was still in progress');
} catch (error) {
	if (!String(error).includes('Timed out opening database')) throw error;
}
const elapsed = Date.now() - start;
// The retry delay is set well above the 1s budget: too-fast means the open
// gave up without actually waiting, too-slow means it silently inherited the
// retry's own timing instead of the configured budget.
if (elapsed < 700 || elapsed > 2500) {
	throw new Error(`Open timeout did not respect lifecycleWaitSeconds (${elapsed}ms)`);
}

// The timed-out opener must not have wedged the retry's own gate -- the
// retry keeps running on its own schedule and still succeeds.
const result = await shutdownResult;
if (!result.shutdown) throw new Error(`Shutdown retry failed: ${JSON.stringify(result)}`);

// And the path must be openable again once the retry actually finishes: the
// timeout must not have left the lifecycle gate stuck closed.
const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value') throw new Error('Retry did not preserve data');
reopened.destroy();
await worker.terminate();
