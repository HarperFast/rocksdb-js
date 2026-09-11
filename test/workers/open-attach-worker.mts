import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Open/attach worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

port.once('message', async () => {
	port.postMessage({ opening: true });
	try {
		const db = RocksDatabase.open(workerData.path);
		const openedBeforeReopen = db.isOpen();
		db.open();
		db.putSync('after-destroy', 'present');
		await db.flush();
		port.postMessage({
			openedBeforeReopen,
			reopened: db.isOpen(),
			value: await db.get('after-destroy'),
		});
		db.close();
	} catch (error) {
		port.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
});
