import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

const [basePath, openCountArg, mode = 'default'] = process.argv.slice(2);
if (!basePath || (mode !== 'default' && mode !== 'cost-to-cache')) {
	console.error(
		'Usage: fork-config-open-snapshot.mts <basePath> [openCount] [default|cost-to-cache]'
	);
	process.exit(1);
}
const openCount = Number(openCountArg ?? 20);
mkdirSync(basePath, { recursive: true });

const KiB = 1024;
const MiB = 1024 * KiB;
// cost-to-cache must be set before the first open creates the WriteBufferManager and charges
// the block cache, so that mode keeps a block cache in both phases (size 0 would leave no cache to charge).
const costToCache = mode === 'cost-to-cache';
const settingsA = {
	blockCacheSize: costToCache ? 8 * MiB : 0,
	writeBufferManagerSize: 128 * MiB,
	...(costToCache ? { writeBufferManagerCostToCache: true } : {}),
	occValidation: 'parallel',
	occLockBuckets: 65536,
	commitThreads: 1,
} as const;
const settingsB = {
	blockCacheSize: costToCache ? 16 * MiB : 8 * MiB,
	writeBufferManagerSize: 512 * MiB,
	...(costToCache ? { writeBufferManagerCostToCache: true } : {}),
	occValidation: 'serial',
	occLockBuckets: 4096,
	commitThreads: 3,
} as const;
const expectedA = {
	sharedBlockCache: costToCache,
	writeBufferManager: true,
	writeBufferManagerCostToCache: costToCache,
	arenaBlockSize: 64 * KiB,
	parallelValidation: true,
	occLockBuckets: 65536,
	commitThreads: 1,
};
const expectedB = {
	sharedBlockCache: true,
	writeBufferManager: true,
	writeBufferManagerCostToCache: costToCache,
	arenaBlockSize: 256 * KiB,
	parallelValidation: false,
	occLockBuckets: 4096,
	commitThreads: 3,
};

RocksDatabase.config(settingsA);

const control = new Int32Array(new SharedArrayBuffer(16));
const worker = new Worker(createWorkerBootstrapScript('./test/workers/config-flip-worker.mts'), {
	eval: true,
	workerData: { control, settingsA, settingsB },
});
const done = new Promise<{ flips?: number; error?: string }>((resolve, reject) => {
	worker.once('message', resolve);
	worker.once('error', reject);
});

const canonical = (o: object) =>
	JSON.stringify(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
const same = (a: object, b: object) => canonical(a) === canonical(b);
const mixed: unknown[] = [];
const seen = { A: 0, B: 0 };
const pinned = { A: false, B: false };
let opens = 0;

async function openOnce(): Promise<object> {
	const i = opens++;
	const path = join(basePath, `db-${i}`);
	const db = RocksDatabase.open(path);
	const captured = (db as any).store.db.getOpenSettingsForTesting();
	await db.transaction(async (txn) => {
		txn.putSync('k', i);
	});
	db.close();
	const reopened = RocksDatabase.open(path);
	if (reopened.getSync('k') !== i) {
		throw new Error(`db-${i} lost its committed write`);
	}
	reopened.close();
	return captured;
}

function record(captured: object) {
	if (same(captured, expectedA)) {
		seen.A++;
	} else if (same(captured, expectedB)) {
		seen.B++;
	} else {
		mixed.push(captured);
	}
}

for (const [phase, name, expected] of [
	[1, 'A', expectedA],
	[2, 'B', expectedB],
] as const) {
	Atomics.store(control, 2, phase);
	Atomics.notify(control, 2);
	for (
		let acked = Atomics.load(control, 3);
		acked !== phase && acked !== -1;
		acked = Atomics.load(control, 3)
	) {
		Atomics.wait(control, 3, acked, 100);
	}
	if (Atomics.load(control, 3) === phase) {
		const captured = await openOnce();
		pinned[name] = same(captured, expected);
		record(captured);
	}
}

Atomics.store(control, 2, 0);
Atomics.notify(control, 2);
while (Atomics.load(control, 1) === 0) {
	Atomics.wait(control, 1, 0, 100);
}
for (let i = 0; i < openCount; i++) {
	record(await openOnce());
}

Atomics.store(control, 0, 1);
Atomics.notify(control, 2);
const { flips, error } = await done;
await worker.terminate();
if (error) {
	throw new Error(`config() worker failed: ${error}`);
}

console.log(`RESULT ${JSON.stringify({ flips, mixed, opens, pinned, seen })}`);
