import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

const incrementer = new BigInt64Array(
	db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
);

const getNextId = () => Atomics.add(incrementer, 0, 1n);

parentPort?.on('message', event => {
	if (event.increment) {
		parentPort?.postMessage({ nextId: getNextId() });
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
