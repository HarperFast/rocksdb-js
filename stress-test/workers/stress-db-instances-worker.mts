import { RocksDatabase } from '../../src/index.js';
import { parentPort, threadId, workerData } from 'node:worker_threads';

const dbs: RocksDatabase[] = [];

for (let i = 0; i < workerData.dbInstances; i++) {
	for (let j = 0; j < workerData.numColumnFamilies; j++) {
		const db = RocksDatabase.open(workerData.path, { name: `table-${j}` });
		await db.transaction(async (transaction) => {
			await transaction.put(`key-${i}-${j}-${threadId}`, 'value');
		});
		dbs.push(db);
	}
}

parentPort?.postMessage({ done: true });

parentPort?.on('message', (event) => {
	if (event.close) {
		for (const db of dbs) {
			db.close();
		}
		parentPort?.postMessage({ closed: true });
	}
});
