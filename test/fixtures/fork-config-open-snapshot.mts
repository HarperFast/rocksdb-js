// A worker flips every open-time config() setting between two sets while this thread opens fresh
// databases; each database must capture one set whole.
import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

const [basePath, openCountArg] = process.argv.slice(2);
if (!basePath) {
	console.error('Usage: fork-config-open-snapshot.mts <basePath> [openCount]');
	process.exit(1);
}
const openCount = Number(openCountArg ?? 20);
// Keeps opening past openCount until both sets have been seen, so a slow worker cannot leave the
// run with only one.
const maxOpens = openCount * 10;
mkdirSync(basePath, { recursive: true });

const KiB = 1024;
const MiB = 1024 * KiB;
const settingsA = {
	blockCacheSize: 0,
	writeBufferManagerSize: 128 * MiB,
	occValidation: 'parallel',
	commitThreads: 1,
} as const;
const settingsB = {
	blockCacheSize: 8 * MiB,
	writeBufferManagerSize: 512 * MiB,
	occValidation: 'serial',
	commitThreads: 3,
} as const;
const expectedA = {
	sharedBlockCache: false,
	writeBufferManager: true,
	arenaBlockSize: 64 * KiB,
	parallelValidation: true,
	commitThreads: 1,
};
const expectedB = {
	sharedBlockCache: true,
	writeBufferManager: true,
	arenaBlockSize: 256 * KiB,
	parallelValidation: false,
	commitThreads: 3,
};

RocksDatabase.config(settingsA);

const control = new Int32Array(new SharedArrayBuffer(8));
const worker = new Worker(createWorkerBootstrapScript('./test/workers/config-flip-worker.mts'), {
	eval: true,
	workerData: { control, settingsA, settingsB },
});
const done = new Promise<{ flips?: number; error?: string }>((resolve, reject) => {
	worker.once('message', resolve);
	worker.once('error', reject);
});
while (Atomics.load(control, 1) === 0) {
	Atomics.wait(control, 1, 0, 100);
}

const same = (a: object, b: object) => JSON.stringify(a) === JSON.stringify(b);
const mixed: unknown[] = [];
const seen = { A: 0, B: 0 };
let opens = 0;
for (let i = 0; i < maxOpens && (i < openCount || seen.A === 0 || seen.B === 0); i++) {
	opens++;
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
	if (same(captured, expectedA)) {
		seen.A++;
	} else if (same(captured, expectedB)) {
		seen.B++;
	} else {
		mixed.push(captured);
	}
}

Atomics.store(control, 0, 1);
const { flips, error } = await done;
await worker.terminate();
if (error) {
	throw new Error(`config() worker failed: ${error}`);
}

console.log(`RESULT ${JSON.stringify({ flips, mixed, opens, seen })}`);
