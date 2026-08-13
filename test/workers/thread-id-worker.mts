import { currentThreadId } from '../../src/index.ts';
import { parentPort } from 'node:worker_threads';

parentPort?.postMessage({ threadId: currentThreadId() });
