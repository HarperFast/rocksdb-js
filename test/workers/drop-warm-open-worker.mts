import { RocksDatabase } from '../../src/index.ts';
import { dropColumnFamilyDelayCountForTesting } from '../../src/load-binding.ts';
import { parentPort, workerData } from 'node:worker_threads';

const { dbPath, columnName, baselineDelayCount, waitTimeoutMs } = workerData as {
	dbPath: string;
	columnName: string;
	baselineDelayCount: number;
	waitTimeoutMs: number;
};

// Warm the descriptor from this thread before the drop, so the timed open below
// measures the registry mutex and not a cold `DB::Open`.
const warm = RocksDatabase.open(dbPath);

// The parent arms the drop latch as soon as it sees this and then drops, so
// this thread must already be watching the counter — a plain loop, because no
// timer runs here. It spins only until the drop enters the latch; the wait for
// the drop to finish happens inside open(), blocked on the registry mutex.
parentPort?.postMessage({ ready: true });

try {
	const deadline = Date.now() + waitTimeoutMs;
	while (dropColumnFamilyDelayCountForTesting() <= baselineDelayCount) {
		if (Date.now() > deadline) {
			throw new Error('timed out waiting for the drop to enter the latch');
		}
	}

	const startedAt = Date.now();
	const db = RocksDatabase.open(dbPath, { name: columnName });
	const openEndedAt = Date.now();
	try {
		db.putSync('written-by-worker', 'value');
		parentPort?.postMessage({
			openEndedAt,
			openDurationMs: openEndedAt - startedAt,
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
