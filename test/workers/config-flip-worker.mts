import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Config flip worker requires a parent port');
const { control, settingsA, settingsB } = workerData as {
	control: Int32Array;
	settingsA: object;
	settingsB: object;
};

let flips = 0;
try {
	do {
		RocksDatabase.config(settingsB as any);
		RocksDatabase.config(settingsA as any);
		if (++flips === 1) {
			Atomics.store(control, 1, 1);
			Atomics.notify(control, 1);
		}
	} while (Atomics.load(control, 0) === 0);
	parentPort.postMessage({ flips });
} catch (error) {
	Atomics.store(control, 1, 1);
	Atomics.notify(control, 1);
	parentPort.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
