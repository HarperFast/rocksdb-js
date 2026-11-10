import { raiseTimestampFloor } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

parentPort?.postMessage({ raised: raiseTimestampFloor(workerData.floor) });
