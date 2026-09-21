import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Column-drop churn worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

port.once('message', () => {
	try {
		// Each drop erases one entry from the process-global descriptor's `columns`
		// map under `columnsMutex`, from a thread that is not the one walking it.
		for (let i = 0; i < workerData.rounds; i++) {
			const column = RocksDatabase.open(workerData.path, { name: `${workerData.prefix}-${i}` });
			column.putSync('key', i);
			column.dropSync();
		}
		port.postMessage({ churned: true });
	} catch (error) {
		port.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
});
