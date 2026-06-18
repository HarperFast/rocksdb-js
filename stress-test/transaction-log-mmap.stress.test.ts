import { transactionLogMapCount } from '../src/load-binding.js';
import { dbRunner, createWorkerBootstrapScript } from '../test/lib/util.js';
import { stressTest } from './setup.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect } from 'vitest';

/**
 * Concurrent read-during-rotation liveness exercise for the transaction-log
 * memory-map weak-reference scheme.
 *
 * A writer thread commits thousands of entries against a tiny max-file-size, so
 * the store rotates its current sequence file hundreds of times. Two reader
 * threads concurrently loop log.query(), which maps the current (actively-
 * written) file. This is exactly the read/rotate concurrency that
 * rotateToNextSequence() now serializes with dataSetsMutex (held across the
 * downgrade + currentSequenceNumber bump, the same lock readers hold while
 * deriving `isCurrent`, so a stale read cannot re-pin a just-frozen file's map).
 *
 * What this asserts: that serialization does not deadlock or livelock under load
 * (the lock change's principal risk — it adds dataSetsMutex under writeMutex; a
 * reader taking the locks in the opposite order would hang here), and that maps
 * do not grossly accumulate — after teardown + GC the live map count collapses
 * to a handful (the current file plus slack) rather than one-per-frozen-file.
 *
 * What this does NOT do: deterministically reproduce the re-pin race itself. The
 * leak window is nanosecond-scale (downgradeMapToFrozen and getMemoryMapLocked
 * are mutually excluded by fileMutex; the only gap is between the writer's
 * fileMutex release and its adjacent advanceSequence()), so a black-box stress
 * test cannot reliably trigger it — a deterministic guard would need a native
 * threaded test with a seam between the downgrade and the sequence bump.
 *
 * POSIX-only: the weak-for-frozen optimization does not apply on Windows (frozen
 * maps are retained strongly by design — see transaction_log_file_windows.cpp).
 */

const bootstrapScript = createWorkerBootstrapScript(
	'./stress-test/workers/stress-transaction-log-mmap-worker.mts'
);

function spawn(path: string, logName: string, role: 'write' | 'read', count: number): Worker {
	return new Worker(bootstrapScript, {
		eval: true,
		workerData: { path, logName, role, count },
	});
}

function onDone(worker: Worker): Promise<void> {
	return new Promise<void>((resolve, reject) => {
		worker.on('error', reject);
		worker.on('message', (event) => {
			if (event.done) {
				resolve();
			}
		});
	});
}

describe('Stress Transaction Log Memory Map', () => {
	stressTest(
		'stays live and releases frozen maps under concurrent reads during rotations',
		{ mode: 'full', skipIf: !globalThis.gc || process.platform === 'win32' },
		() =>
			// Small max file size so ~200B entries rotate the current file every few
			// commits; 4000 writes ⇒ hundreds of rotations under concurrent reads.
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				const logName = 'race';
				// Create the store (with the small max size) on the main thread before
				// the workers attach to the same process-global store.
				db.useLog(logName);

				const writer = spawn(dbPath, logName, 'write', 4000);
				const readers = [spawn(dbPath, logName, 'read', 0), spawn(dbPath, logName, 'read', 0)];

				// Wait for the writer to finish all rotations.
				await onDone(writer);

				// Stop the readers and wait for them to acknowledge.
				const readersDone = readers.map(onDone);
				for (const reader of readers) {
					reader.postMessage({ stop: true });
				}
				await Promise.all(readersDone);

				// Tear down every worker. Terminating the reader envs runs their
				// external-buffer finalizers (unmapping any maps they still held), so
				// the only mappings that can survive are ones a TransactionLogFile is
				// retaining with a strong reference.
				await Promise.all([writer, ...readers].map((w) => w.terminate()));

				// Force GC + give finalizers time to run, then the live map count must
				// have collapsed: frozen files keep only weak handles. A re-pin leak
				// would leave one strong map per re-pinned frozen file (dozens+).
				for (let i = 0; i < 5; i++) {
					globalThis.gc!();
					await delay(50);
				}

				const liveMaps = transactionLogMapCount();
				expect(liveMaps).toBeLessThanOrEqual(5);
			})
	);
});
