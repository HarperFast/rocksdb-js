import { RocksDatabase, shutdown } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

RocksDatabase.open(workerData.path);
if (!parentPort) throw new Error('Shutdown/compact-cancel worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

port.once('message', () => {
	port.postMessage({ shuttingDown: true });
	try {
		shutdown();
		port.postMessage({ shutdown: true });
	} catch (error) {
		port.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
});
