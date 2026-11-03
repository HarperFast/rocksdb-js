import { RocksDatabase, raiseTimestampFloor } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// A child process, because the floor is process-wide: raising it an hour ahead
// in the test runner would skew every other test file's timestamps.
const [mode, dbPath, floorArg] = process.argv.slice(2);
const floor = Number(floorArg);
const LOG = 'local';

let raised: boolean;
if (mode === 'worker') {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/raise-timestamp-floor-worker.mts'),
		{ eval: true, workerData: { floor } }
	);
	({ raised } = await new Promise<{ raised: boolean }>((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
		worker.once('exit', (code) => reject(new Error(`worker exited early (${code})`)));
	}));
	await worker.terminate();
} else {
	raised = raiseTimestampFloor(floor);
}

const db = RocksDatabase.open(dbPath, mode === 'seeded' ? { timestampFloorLog: LOG } : {});
try {
	const clock = db.getMonotonicTimestamp();
	let txnTimestamp = 0;
	await db.transaction(async (txn) => {
		txnTimestamp = txn.getTimestamp();
		await txn.put('k', 'v');
		db.useLog(LOG).addEntry(Buffer.from('entry'), txn.id);
	});
	const logTimestamps: number[] = [];
	for (const entry of db.useLog(LOG).query({ start: 0 })) {
		logTimestamps.push(entry.timestamp);
	}
	console.log(JSON.stringify({ raised, clock, txnTimestamp, logTimestamps }));
} finally {
	db.close();
}
