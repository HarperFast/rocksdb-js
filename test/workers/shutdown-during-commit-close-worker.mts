import { RocksDatabase, shutdown } from '../../src/index.ts';
import {
	isTransactionCloseDelayedForTesting,
	isTransactionCloseGuardedAtDrainForTesting,
} from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

// The parent's JS thread is blocked inside its commit completion's close(), so this worker cannot be
// messaged; it watches the native close-latch flag instead.
RocksDatabase.open(workerData.path);
if (!parentPort) throw new Error('Shutdown-during-commit-close worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const deadline = Date.now() + 10_000;
while (!isTransactionCloseDelayedForTesting()) {
	if (Date.now() > deadline) {
		port.postMessage({ error: 'the commit completion never reached the close latch' });
		process.exit(1);
	}
	Atomics.wait(sleepCell, 0, 0, 2);
}
try {
	shutdown();
	port.postMessage({
		shutdown: true,
		closeGuardedAtDrain: isTransactionCloseGuardedAtDrainForTesting(),
	});
} catch (error) {
	port.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
