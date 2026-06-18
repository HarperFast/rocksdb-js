import { RocksDatabase } from '../../src/index.js';
import { setImmediate as yieldTick } from 'node:timers/promises';
import { parentPort, workerData } from 'node:worker_threads';

// Worker for the concurrent read-during-rotation regression test
// (transaction-log-mmap.stress.test.ts). Two roles share one process-global
// TransactionLogStore (keyed by db path + log name):
//
//   'write' — commits `count` log entries against a small max-file-size, so the
//             store rotates the current sequence file constantly.
//   'read'  — loops log.query(), which maps the current (actively-written) file,
//             until the parent signals stop.
//
// The writer's rotation (downgradeMapToFrozen + sequence bump) racing the
// reader's getMemoryMap()/findPositionByTimestamp() is exactly the interleaving
// the dataSetsMutex hold in rotateToNextSequence() must serialize. Single-thread
// JS cannot reproduce it — the native calls are individually atomic on one
// thread — so the race only surfaces across real OS threads.

const { path, logName, role, count } = workerData as {
	path: string;
	logName: string;
	role: 'write' | 'read';
	count: number;
};

const db = RocksDatabase.open(path);
const log = db.useLog(logName);

let stop = false;
parentPort?.on('message', (event) => {
	if (event.stop) {
		stop = true;
	}
});

if (role === 'write') {
	// ~200B payload against the small max file size set by the parent rotates
	// the current sequence file every few commits.
	const payload = Buffer.alloc(200, 0x61);
	for (let i = 0; i < count; i++) {
		await db.transaction(async (txn) => {
			log.addEntry(payload, txn.id);
		});
	}
	parentPort?.postMessage({ done: true });
} else {
	// Continuously read to the latest committed position; each pass maps the
	// current file (and walks frozen files) until told to stop.
	let passes = 0;
	while (!stop) {
		for (const _entry of log.query()) {
			// drain — iterating forces the memory maps to be acquired
		}
		passes++;
		await yieldTick();
	}
	parentPort?.postMessage({ done: true, passes });
}
