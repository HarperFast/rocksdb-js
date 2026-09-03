import { RocksDatabase } from '../../src/index.ts';
import { parentPort, workerData } from 'node:worker_threads';

// Continuous overwrite + flush traffic against the primary: every flush adds
// L0 files, every fourth one triggers an L0->L1 compaction that deletes its
// input SSTs, and the >= 2KB values keep blob GC rewriting and deleting blob
// files — the deletions the parent's read-only opens race.
const db = new RocksDatabase(workerData.dbPath, {
	writeBufferSize: workerData.writeBufferSize,
});
db.open();

const value = 'v'.repeat(workerData.valueSize);
let stop = false;
parentPort!.on('message', (msg) => {
	if (msg === 'stop') {
		stop = true;
	}
});

let iteration = 0;
function loop(): void {
	if (stop) {
		db.close();
		parentPort!.postMessage('stopped');
		return;
	}
	for (let j = 0; j < workerData.keys; j++) {
		db.putSync(`key${j}`, value + iteration + j);
	}
	db.flushSync({ allowWriteStall: true });
	iteration++;
	setImmediate(loop);
}
parentPort!.postMessage('ready');
loop();
