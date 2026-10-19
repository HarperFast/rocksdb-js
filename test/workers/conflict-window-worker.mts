import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Conflict-window worker requires a parent port');
const port = parentPort;

// Issues one non-transactional write to the key a logged commit holds reserved
// on the main thread. The write must wait for that commit's window to end.
const db = RocksDatabase.open(workerData.path);
try {
	if (workerData.delayMs > 0) {
		await new Promise((resolve) => setTimeout(resolve, workerData.delayMs));
	}
	const started = Date.now();
	db.putSync(workerData.key, workerData.value);
	port.postMessage({ started, done: Date.now() });
} catch (error) {
	port.postMessage({ error: error instanceof Error ? error.message : String(error) });
} finally {
	db.close();
}
