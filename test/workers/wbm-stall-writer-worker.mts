import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { path, columnFamilies, maintain } = workerData as {
	path: string;
	columnFamilies: number;
	maintain: number;
};

const dbs = Array.from({ length: columnFamilies }, (_, i) =>
	RocksDatabase.open(path, { name: `cf${i}`, maxWriteBufferSizeToMaintain: maintain })
);

const value = Buffer.alloc(64 * 1024, 1);
parentPort?.postMessage({ ready: true });

// putSync blocks inside RocksDB once the manager stalls, which is the point: this
// thread parks in the stall while the main thread stays free to observe it.
for (let i = 0; ; i++) {
	dbs[i % dbs.length].putSync(`stall-${i.toString().padStart(7, '0')}`, value);
}
