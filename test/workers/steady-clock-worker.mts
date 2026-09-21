import { steadyClockNow } from '../../src/index.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { parentPort, workerData } from 'node:worker_threads';

const first = steadyClockNow();
await delay(workerData?.sleepMs ?? 30);
const second = steadyClockNow();

parentPort?.postMessage({ first, second });
