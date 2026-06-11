import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path, { name: workerData.name });

// Touch the same shared mapping as the main thread (registry-backed).
db.getUserSharedBuffer(workerData.key, new ArrayBuffer(8));

parentPort?.on('message', (event: { close?: boolean }) => {
	if (event.close) {
		db.close();
		parentPort?.postMessage({ closed: true });
	}
});

parentPort?.postMessage({ started: true });
