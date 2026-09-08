import { shutdown } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const gate = new Int32Array(workerData.gate as SharedArrayBuffer);

parentPort!.postMessage('ready');
Atomics.wait(gate, 0, 0);
shutdown();
