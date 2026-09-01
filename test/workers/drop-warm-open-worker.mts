import { RocksDatabase } from '../../src/index.ts';
import { dropColumnFamilyDelayCountForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath, columnName, waitTimeoutMs } = workerData as {
	dbPath: string;
	columnName: string;
	waitTimeoutMs: number;
};

// Warm the descriptor from this thread before the drop, so the timed open below
// measures the registry mutex and not a cold `DB::Open`.
const warm = RocksDatabase.open(dbPath);

// Signal readiness, then spin — the parent arms the drop seam once it sees this
// and immediately drops, so this thread has to be watching the counter already
// rather than waiting on its own event loop for a second message. The spin is
// a plain loop for the same reason: no timer runs here. It lasts only until the
// parent's drop enters the seam (milliseconds); the wait for the drop to finish
// is inside open(), blocked on the registry mutex.
parentPort?.postMessage({ ready: true });

try {
	// Once the drop is provably inside its critical section, open the family it
	// is dropping. With the section intact this open blocks on the registry
	// mutex until the drop has erased the column entry, so it creates a fresh,
	// writable family; if the mutex were released mid-section it would return
	// the dropped family and the write below would be silently discarded.
	const deadline = Date.now() + waitTimeoutMs;
	while (dropColumnFamilyDelayCountForTesting() === 0) {
		if (Date.now() > deadline) {
			throw new Error('timed out waiting for the drop to enter the delay');
		}
	}

	const startedAt = Date.now();
	const db = RocksDatabase.open(dbPath, { name: columnName });
	const openDurationMs = Date.now() - startedAt;
	try {
		db.putSync('written-by-worker', 'value');
		parentPort?.postMessage({
			openDurationMs,
			readBack: db.getSync('written-by-worker'),
			columns: db.columns,
		});
	} finally {
		db.close();
	}
} catch (err) {
	parentPort?.postMessage({ error: (err as Error).message });
} finally {
	warm.close();
}
