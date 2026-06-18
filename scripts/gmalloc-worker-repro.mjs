import { rmSync } from 'node:fs';
import { join } from 'node:path';
// Tight repro of the cross-env shared-DBDescriptor teardown path that the
// worker benchmarks exercise. Like the benches (benchmark/setup.ts), every
// worker opens the SAME database path, so all worker envs share one refcounted
// DBDescriptor in the process-global DBRegistry and tear it down concurrently
// on exit. This loops that open/work/exit cycle far faster than the full vitest
// bench sampling, to surface the intermittent heap corruption under Guard
// Malloc quickly.
//
// Run under Guard Malloc:
//   DYLD_INSERT_LIBRARIES=/usr/lib/libgmalloc.dylib MallocScribble=1 \
//     node scripts/gmalloc-worker-repro.mjs
import { Worker, isMainThread, workerData, parentPort } from 'node:worker_threads';

const NUM_WORKERS = Number(process.env.WORKERS || 10);
const ROUNDS = Number(process.env.ROUNDS || 200);
const PUTS = Number(process.env.PUTS || 200);

if (!isMainThread) {
	const { path, mode } = workerData;
	const { RocksDatabase } = await import('../dist/index.mjs');
	const db = RocksDatabase.open(path, { disableWAL: true });
	const payload = Buffer.alloc(64, 'x');
	for (let i = 0; i < PUTS; i++) {
		const key = String((Math.random() * 5000) | 0);
		db.putSync(key, payload);
	}
	// Half the workers close explicitly; the other half exit with the DB still
	// open. Both then process.exit() (like the real bench workers), forcing the
	// env-cleanup hook to tear down the shared descriptor — concurrently across
	// all worker envs, which is the suspected corruption window.
	if (mode === 'close') {
		await db.close();
	}
	parentPort.postMessage('done');
	process.exit(0);
} else {
	for (let r = 0; r < ROUNDS; r++) {
		const path = join('benchmark', 'data', `gmalloc-repro-${process.pid}-${r}`);
		rmSync(path, { recursive: true, force: true });
		await Promise.all(
			Array.from({ length: NUM_WORKERS }, (_, i) => {
				return new Promise((resolve, reject) => {
					const w = new Worker(new URL(import.meta.url), {
						workerData: { path, mode: i % 2 === 0 ? 'close' : 'leave' },
					});
					w.on('error', reject);
					w.on('exit', () => resolve());
				});
			})
		);
		rmSync(path, { recursive: true, force: true });
		if (r % 10 === 0) console.log(`round ${r}/${ROUNDS} ok`);
	}
	console.log('WORKER REPRO COMPLETE — no crash');
}
