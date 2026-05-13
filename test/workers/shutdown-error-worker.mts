import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

let writeCount = 0;

// Continuously write data until we get an error
async function writeLoop() {
	while (true) {
		try {
			const key = `key-${writeCount}`;
			const value = { index: writeCount, data: `value-${writeCount}` };
			db.putSync(key, value);
			writeCount++;

			// Report progress periodically
			if (writeCount % 100 === 0) {
				parentPort?.postMessage({ writeCount });
			}
		} catch (error) {
			// Report the error back to the main thread
			parentPort?.postMessage({
				error: true,
				message: error instanceof Error ? error.message : String(error),
				writeCount,
			});
			return;
		}
	}
}

parentPort?.postMessage({ started: true });

// Start the write loop
writeLoop();
