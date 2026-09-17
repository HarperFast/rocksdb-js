import { RocksDatabase, steadyClockNow } from '../../src/index.ts';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';

const db = new RocksDatabase(process.argv[2]).open();
const sleepMs = 100;
function sample() {
	process.stdout.write(
		JSON.stringify({
			wall: Date.now(),
			monotonic: db.getMonotonicTimestamp(),
			steady: steadyClockNow(),
			sleepMs,
		}) + '\n'
	);
}

const lines = createInterface({ input: process.stdin });
try {
	sample();
	for await (const line of lines) {
		if (line.trim() !== 'step') throw new Error('unexpected sample command');
		await delay(sleepMs);
		sample();
	}
} finally {
	lines.close();
	db.close();
}
