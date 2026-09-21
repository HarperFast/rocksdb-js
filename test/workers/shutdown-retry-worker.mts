import { shutdown } from '../../src/index.ts';
import { parentPort } from 'node:worker_threads';

if (!parentPort) throw new Error('Shutdown retry worker requires a parent port');
parentPort.postMessage(Date.now());
try {
	shutdown();
	parentPort.postMessage({ shutdown: true });
} catch (error) {
	parentPort.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
