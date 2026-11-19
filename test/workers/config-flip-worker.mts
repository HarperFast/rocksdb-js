import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) throw new Error('Config flip worker requires a parent port');
const { control, settingsA, settingsB } = workerData as {
	control: Int32Array;
	settingsA: object;
	settingsB: object;
};

// control: [0] stop, [1] flipped once, [2] phase (0 free-running, 1 pin A, 2 pin B), [3] phase acked
let flips = 0;
try {
	while (Atomics.load(control, 0) === 0) {
		const phase = Atomics.load(control, 2);
		if (phase === 0) {
			RocksDatabase.config(settingsB as any);
			RocksDatabase.config(settingsA as any);
			if (++flips === 1) {
				Atomics.store(control, 1, 1);
				Atomics.notify(control, 1);
			}
		} else {
			RocksDatabase.config((phase === 1 ? settingsA : settingsB) as any);
			Atomics.store(control, 3, phase);
			Atomics.notify(control, 3);
			Atomics.wait(control, 2, phase, 100);
		}
	}
	parentPort.postMessage({ flips });
} catch (error) {
	Atomics.store(control, 1, 1);
	Atomics.store(control, 3, -1);
	Atomics.notify(control, 1);
	Atomics.notify(control, 3);
	parentPort.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
