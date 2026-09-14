import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Open the SAME path as the parent so both envs share one DBDescriptor and its
// lock table. The parent holds `key`; this tryLock therefore fails and queues an
// unlock callback -- a threadsafe function bound to THIS env -- on the shared
// LockHandle. The parent then terminates this worker and unlocks.
const db = RocksDatabase.open(workerData.path);
const acquired = db.tryLock(workerData.key, () => {
	parentPort?.postMessage({ fired: true });
});
parentPort?.postMessage({ ready: true, acquired });
// Stay alive until terminated; the callback must still be queued at teardown.
setInterval(() => {}, 1000);
