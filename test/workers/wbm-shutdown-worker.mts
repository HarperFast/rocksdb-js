import { shutdown } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const gate = new Int32Array(workerData.gate as SharedArrayBuffer);

// Load the binding first, then block until every worker is at the gate, so the
// shutdown calls overlap instead of queueing behind each other's module init.
parentPort!.postMessage('ready');
Atomics.wait(gate, 0, 0);
shutdown();
