import { currentThreadId } from '../../src/index.js';
import { parentPort } from 'node:worker_threads';

parentPort?.postMessage({ threadId: currentThreadId() });
