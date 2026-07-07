import { fileLockRelease, tryFileLock } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

const { file } = workerData;

parentPort?.on('message', (event) => {
	if (event.tryAcquire) {
		const token = tryFileLock(file);
		parentPort?.postMessage({ tryAcquire: token });
	} else if (event.acquire) {
		const token = tryFileLock(file);
		parentPort?.postMessage({ acquired: token });
	} else if (event.release !== undefined) {
		fileLockRelease(event.release);
		parentPort?.postMessage({ released: true });
	} else if (event.close) {
		process.exit(0);
	}
});

parentPort?.postMessage({ ready: true });
