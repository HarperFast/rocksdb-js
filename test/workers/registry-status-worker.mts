import { registryStatus } from '../../src/index.ts';
import { parentPort } from 'node:worker_threads';

if (!parentPort) throw new Error('Registry-status worker requires a parent port');
const port = parentPort;
port.postMessage({ ready: true });

port.once('message', () => {
	try {
		port.postMessage({ started: true });
		registryStatus();
		port.postMessage({ finished: true });
	} catch (error) {
		port.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
});
