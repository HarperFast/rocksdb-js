import { RocksDatabase } from '../../src/index.ts';
import { dropColumnFamilyLatchStatsForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath, columnName, baselineEntered, waitTimeoutMs } = workerData as {
	dbPath: string;
	columnName: string;
	baselineEntered: number;
	waitTimeoutMs: number;
};

// Warm the descriptor from this thread before the drop, so the open below is a
// warm registry lookup and not a cold `DB::Open`.
const warm = RocksDatabase.open(dbPath);

// The parent arms the drop latch as soon as it sees this and then drops, so
// this thread must already be watching the counter — a plain loop, because no
// timer runs here. It spins only until the drop parks in the latch.
parentPort?.postMessage({ ready: true });

try {
	const deadline = Date.now() + waitTimeoutMs;
	while (dropColumnFamilyLatchStatsForTesting().entered <= baselineEntered) {
		if (Date.now() > deadline) {
			throw new Error('timed out waiting for the drop to park in the latch');
		}
	}

	// The parked drop is holding the registry mutex, and waits for this open to
	// reach it before holding on any further. With the critical section intact
	// this open cannot proceed until the drop has erased the column entry, so it
	// creates a fresh, writable family; if the mutex were released mid-section
	// it would get the dropped family and the write below would be discarded.
	const db = RocksDatabase.open(dbPath, { name: columnName });
	try {
		db.putSync('written-by-worker', 'value');
		parentPort?.postMessage({
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
