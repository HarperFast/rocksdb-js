import { RocksDatabase } from '../../src/index.ts';
import { parkBehind } from '../lib/park.ts';
import { workerData } from 'node:worker_threads';

// Same path as the parent, so this park lands on the tracker of the lock the parent holds. The
// parent terminates this worker while the park is pending.
const db = RocksDatabase.open(workerData.path, { encoding: false, verificationTable: true });
await parkBehind(db, Buffer.from(workerData.key));
setInterval(() => {}, 1000);
