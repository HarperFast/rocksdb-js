import { RocksDatabase, shutdown } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path, {
	disableWAL: true,
});

let writeCount = 0;
let running = true;

// Continuously write data until told to stop
async function writeLoop() {
	while (running) {
		const key = `key-${writeCount}`;
		const value = { index: writeCount, data: `value-${writeCount}` };
		await db.put(key, value);
		writeCount++;

		// Report progress periodically
		if (writeCount % 100 === 0) {
			parentPort?.postMessage({ writeCount });
		}
	}
}

parentPort?.on('message', (event) => {
	if (event.stop) {
		running = false;
		// Report final count before shutdown
		parentPort?.postMessage({ writeCount, stopped: true });
		shutdown();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });

// Start the write loop
writeLoop();
