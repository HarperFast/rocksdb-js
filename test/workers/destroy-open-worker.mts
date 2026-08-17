import { RocksDatabase } from '../../src/index.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);
if (!parentPort) throw new Error('Destroy/open worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

port.once('message', async () => {
	port.postMessage({ destroying: true });
	if (workerData.destroyStartDelayMs > 0) await delay(workerData.destroyStartDelayMs);
	try {
		db.destroy();
		port.postMessage({ destroyed: true });
	} catch (error) {
		port.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
});
