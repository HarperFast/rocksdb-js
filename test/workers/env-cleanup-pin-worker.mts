import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Loads the binding into this env and leaves it no handle on the database, so its env-cleanup hook
// only pins the shared descriptor.
RocksDatabase.open(workerData.path).close();
parentPort!.postMessage({ closed: true });
process.exit(0);
