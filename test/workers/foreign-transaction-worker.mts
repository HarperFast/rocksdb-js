import { RocksDatabase, Transaction } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const db = RocksDatabase.open(workerData.path);

function attempt(fn: () => unknown): { code?: string; message?: string } {
	try {
		fn();
		return {};
	} catch (error) {
		return { code: (error as any).code, message: (error as Error).message };
	}
}

async function attemptAsync(fn: (resolve: () => void, reject: (error: Error) => void) => void) {
	try {
		await new Promise<void>(fn);
		return {};
	} catch (error) {
		return { code: (error as any).code, message: (error as Error).message };
	}
}

async function attemptAll(
	id: number
): Promise<Record<string, { code?: string; message?: string }>> {
	// `store` is what Store.getTxnId() checks; this worker's store has the same identity
	const transaction = { id, store: db.store } as any;
	const native = db.store.db as any;
	return {
		get: await attemptAsync((resolve, reject) => native.get(Buffer.from('k'), resolve, reject, id)),
		getSync: attempt(() => db.getSync('k', { transaction })),
		getCount: attempt(() => db.getKeysCount({ transaction })),
		putSync: attempt(() => db.putSync('foreign', 'x', { transaction })),
		removeSync: attempt(() => db.removeSync('k', { transaction })),
		getRange: attempt(() => db.getRange({ transaction })),
		addEntry: attempt(() => db.useLog('foo').addEntry(Buffer.from('x'), id)),
	};
}

parentPort?.on('message', async (event) => {
	if (event.txnId !== undefined) {
		const foreign = await attemptAll(event.txnId);
		// control: a transaction this worker created is usable by id here
		const own = new Transaction(db.store);
		const local = await attemptAll(own.id);
		own.abort();
		parentPort?.postMessage({ foreign, local });
	} else if (event.close) {
		db.close();
		process.exit(0);
	}
});

parentPort?.postMessage({ started: true });
