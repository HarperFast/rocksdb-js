import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

// The parent blocks inside native calls and cannot receive messages, so
// progress is reported through shared memory (see fork-sync-operation-destroy.mts).
const DESTROY = 0;
const HOLDER = 1;
const RELEASE = 2;

const db = RocksDatabase.open(workerData.path);
if (!parentPort) throw new Error('Sync operation destroy worker requires a parent port');
const port = parentPort;
const state = new Int32Array(workerData.state as SharedArrayBuffer);
port.postMessage({ ready: true });

const signal = (index: number, value: number) => {
	Atomics.store(state, index, value);
	Atomics.notify(state, index);
};

port.on('message', (message: string) => {
	if (message === 'hold') {
		let holdError: unknown;
		try {
			(db as any).store.db.getCount(
				{
					get exclusiveStart() {
						signal(HOLDER, 1);
						Atomics.wait(state, RELEASE, 0);
						return false;
					},
				},
				undefined
			);
		} catch (error) {
			holdError = error;
		}
		signal(HOLDER, 2);
		port.postMessage({ held: true, error: holdError ? String(holdError) : undefined });
	} else if (message === 'destroy') {
		signal(DESTROY, 1);
		try {
			db.destroy();
			port.postMessage({ destroyed: true });
		} catch (error) {
			port.postMessage({ error: error instanceof Error ? error.message : String(error) });
		} finally {
			signal(DESTROY, 2);
		}
	}
});
