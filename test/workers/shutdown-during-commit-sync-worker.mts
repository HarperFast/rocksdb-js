import { RocksDatabase, shutdown } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

// The parent's JS thread is blocked inside commitSync(), so this worker cannot be messaged; it
// watches the native commit-execute delay flag instead.
RocksDatabase.open(workerData.path);
if (!parentPort) throw new Error('Shutdown-during-commitSync worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const deadline = Date.now() + 10_000;
while (!isTransactionCommitExecuteDelayedForTesting()) {
	if (Date.now() > deadline) {
		port.postMessage({ error: 'commitSync() never reached the commit-execute delay' });
		process.exit(1);
	}
	Atomics.wait(sleepCell, 0, 0, 2);
}
try {
	shutdown();
	port.postMessage({
		shutdown: true,
		at: Date.now(),
		delayStillActive: isTransactionCommitExecuteDelayedForTesting(),
	});
} catch (error) {
	port.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
