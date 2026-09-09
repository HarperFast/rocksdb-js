import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A short lifecycleWaitSeconds budget makes OpenDB's "close retry in
// progress" wait (db_registry.cpp) time out well before a close retry --
// deliberately slowed by ROCKSDB_JS_CLOSE_RETRY_DELAY_MS -- can finish,
// exercising the timeout throw path itself and proving recovery afterward:
// none of the `lifecycleWaitSeconds` timeout branches had a test before this
// (only config validation did). This is a single descriptor on the path, so
// it does not reproduce a predicate/notifier mismatch across two concurrently
// closing descriptors (db_registry.cpp:585's own fix needs a dedicated
// two-descriptor test) -- it only proves a timeout fires and the gate
// recovers afterward.
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
