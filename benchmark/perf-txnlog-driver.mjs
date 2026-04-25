// Tight driver for profiling/benchmarking transaction log writeBatch.
import { RocksDatabase } from '../dist/index.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const DISABLE_WAL = process.env.DISABLE_WAL !== '0';
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 8);
const WARMUP = 2000;
const ITERS = 50_000;

const dir = mkdtempSync(join(tmpdir(), 'txnlog-perf-'));
const db = await RocksDatabase.open(dir, { disableWAL: DISABLE_WAL });
const log = db.useLog('perf');
const data = Buffer.alloc(100, 0xab);

for (let i = 0; i < WARMUP; i++) {
	await db.transaction((txn) => { log.addEntry(data, txn.id); });
}

console.log(`=== profiling window start (disableWAL=${DISABLE_WAL}, concurrency=${CONCURRENCY}) ===`);
const start = performance.now();

async function worker() {
	for (let i = 0; i < ITERS / CONCURRENCY; i++) {
		await db.transaction((txn) => { log.addEntry(data, txn.id); });
	}
}

await Promise.all(Array.from({ length: CONCURRENCY }, worker));

const elapsed = performance.now() - start;
console.log(`=== profiling window end ===`);
console.log(`${ITERS} commits in ${elapsed.toFixed(1)}ms = ${(ITERS / (elapsed / 1000)).toFixed(0)} commits/sec`);

db.close();
rmSync(dir, { recursive: true });
