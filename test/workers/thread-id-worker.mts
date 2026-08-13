import { currentThreadId } from '../../dist/index.mjs';
import { parentPort } from 'node:worker_threads';

parentPort?.postMessage({ threadId: currentThreadId() });
