import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const { path, name, key, cycles } = workerData as {
	path: string;
	name: string;
	key: string;
	cycles: number;
};

parentPort?.on('message', (event: { run?: boolean }) => {
	if (!event.run) {
		return;
	}

	for (let i = 0; i < cycles; i++) {
		const db = RocksDatabase.open(path, { name });
		db.getUserSharedBuffer(key, new ArrayBuffer(8));
		db.close();
	}

	parentPort?.postMessage({ done: true });
});

parentPort?.postMessage({ started: true });
