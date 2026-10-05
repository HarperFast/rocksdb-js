// Bucket counts are process-wide settings applied at open, so each mode runs in a fresh process.
import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { TransactionIsBusyError } from '../../src/transaction.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { mkdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const options = { noBlockCache: true, writeBufferSize: 1024 * 1024, parallelismThreads: 1 };
const counterIncrements = 64;

function anonymousMiB(): number | null {
	if (process.platform !== 'linux') return null;
	return (
		Number(/Anonymous:\s+(\d+)/.exec(readFileSync('/proc/self/smaps_rollup', 'utf8'))![1]) / 1024
	);
}

function openMany(root: string, prefix: string, count: number) {
	const before = anonymousMiB();
	const dbs = Array.from({ length: count }, (_, i) =>
		RocksDatabase.open(join(root, `${prefix}${i}`), options)
	);
	const after = anonymousMiB();
	return { dbs, growth: before === null || after === null ? null : after - before };
}

// Increments a shared counter with a read-modify-write transaction, retrying on conflict. Lost
// updates would mean a true write-write conflict went undetected.
async function incrementCounter(db: RocksDatabase, sync: boolean): Promise<number> {
	for (let conflicts = 0; ; conflicts++) {
		const txn = new Transaction(db.store);
		try {
			txn.putSync('counter', ((txn.getSync('counter') as number | undefined) ?? 0) + 1);
			if (sync) txn.commitSync();
			else await txn.commit();
			return conflicts;
		} catch (error) {
			txn.abort();
			if (!(error instanceof TransactionIsBusyError)) throw error;
		}
	}
}

if (!isMainThread) {
	const { path, id } = workerData as { path: string; id: number };
	const db = RocksDatabase.open(path, options);
	try {
		parentPort!.postMessage('ready');
		await new Promise<void>((resolve) => parentPort!.once('message', resolve));
		// Worker 0 commits through the database's commit lane, worker 1 with commitSync on its own
		// thread: the one pairing that can contend for one database's buckets.
		const sync = id === 1;
		let conflicts = 0;
		for (let round = 0; round < counterIncrements; round++) {
			const txn = new Transaction(db.store);
			for (let key = 0; key < 64; key++) txn.putSync(`w${id}-${key}`, round);
			if (sync) txn.commitSync();
			else await txn.commit();
			conflicts += await incrementCounter(db, sync);
		}
		for (let key = 0; key < 64; key++) {
			assert.equal(db.getSync(`w${id}-${key}`), counterIncrements - 1);
		}
		parentPort!.postMessage({ conflicts });
	} finally {
		db.close();
	}
} else {
	const [root, mode] = process.argv.slice(2);
	assert.ok(root);
	assert.ok(['default', 'configured', 'transactions'].includes(mode));
	mkdirSync(root, { recursive: true });
	const result: Record<string, number | null> = {};

	if (mode === 'default') {
		const { dbs, growth } = openMany(root, 'db', 10);
		for (const db of dbs.reverse()) db.close();
		result.defaultGrowthMiB = growth;
	}

	if (mode === 'configured') {
		RocksDatabase.config({ occLockBuckets: 1 << 20 });
		const large = openMany(root, 'large', 3);
		RocksDatabase.config({ occLockBuckets: 16 });
		// A database opened before the change keeps its count; this open must reuse it.
		const reused = RocksDatabase.open(join(root, 'large0'), options);
		const small = openMany(root, 'small', 3);
		reused.putSync('key', 'value');
		assert.equal(large.dbs[0].getSync('key'), 'value');
		for (const db of [reused, ...small.dbs, ...large.dbs]) db.close();
		result.largeGrowthMiB = large.growth;
		result.smallGrowthMiB = small.growth;
	}

	if (mode === 'transactions') {
		for (const invalid of [0, -1, 15, 1.5, NaN, Infinity, -Infinity, 16777217, 2 ** 32]) {
			assert.throws(() => RocksDatabase.config({ occLockBuckets: invalid }), RangeError);
		}
		for (const invalid of ['16', true, {}, 16n]) {
			assert.throws(() => RocksDatabase.config({ occLockBuckets: invalid } as any), TypeError);
		}
		RocksDatabase.config({ occLockBuckets: 16777216 });
		RocksDatabase.config({ occLockBuckets: undefined });
		RocksDatabase.config({ occLockBuckets: null } as any);
		RocksDatabase.config({ occLockBuckets: 16 });

		const pessimisticPath = join(root, 'pessimistic');
		const pessimistic = RocksDatabase.open(pessimisticPath, { ...options, pessimistic: true });
		pessimistic.putSync('key', 'value');
		pessimistic.close();
		const readOnly = RocksDatabase.open(pessimisticPath, { ...options, readOnly: true });
		assert.equal(readOnly.getSync('key'), 'value');
		readOnly.close();

		const dbs = [0, 1].map((i) => RocksDatabase.open(join(root, `db${i}`), options));
		const other = RocksDatabase.open(join(root, 'db0'), { ...options, name: 'other' });
		try {
			// 1,024 keys over 16 buckets: every bucket is shared many times within one commit.
			for (const sync of [false, true]) {
				const txn = new Transaction(dbs[0].store);
				for (let i = 0; i < 1024; i++) txn.putSync(`bulk-${sync}-${i}`, i);
				if (sync) txn.commitSync();
				else await txn.commit();
				for (let i = 0; i < 1024; i++) assert.equal(dbs[0].getSync(`bulk-${sync}-${i}`), i);
			}
			// The same key in different databases and column families is not a conflict.
			const txns = [dbs[0], dbs[1], other].map((db, i) => {
				const txn = new Transaction(db.store);
				txn.putSync('same-key', i);
				return txn;
			});
			await Promise.all(txns.map((txn) => txn.commit()));
			[dbs[0], dbs[1], other].forEach((db, i) => assert.equal(db.getSync('same-key'), i));
			for (const sync of [false, true]) {
				const loser = new Transaction(dbs[0].store);
				try {
					loser.putSync('conflict', 'loser');
					dbs[0].putSync('conflict', 'winner');
					if (sync) assert.throws(() => loser.commitSync(), TransactionIsBusyError);
					else await assert.rejects(loser.commit(), TransactionIsBusyError);
					assert.equal(dbs[0].getSync('conflict'), 'winner');
				} finally {
					loser.abort();
				}
			}
		} finally {
			other.close();
			for (const db of dbs) db.close();
		}

		if (!process.versions.deno) {
			const path = join(root, 'shared');
			const db = RocksDatabase.open(path, options);
			const workers = [0, 1].map(
				(id) =>
					new Worker(createWorkerBootstrapScript(fileURLToPath(import.meta.url)), {
						eval: true,
						workerData: { path, id },
					})
			);
			const exited = workers.map(
				(worker) =>
					new Promise<void>((resolve, reject) => {
						worker.once('error', reject);
						worker.once('exit', (code) =>
							code === 0 ? resolve() : reject(new Error(`Worker exit ${code}`))
						);
					})
			);
			await Promise.all(
				workers.map(
					(worker) =>
						new Promise<void>((resolve, reject) => {
							worker.once('error', reject);
							worker.once('message', () => resolve());
						})
				)
			);
			const results = workers.map(
				(worker) => new Promise<{ conflicts: number }>((resolve) => worker.once('message', resolve))
			);
			for (const worker of workers) worker.postMessage('start');
			const messages = await Promise.all(results);
			await Promise.all(exited);
			assert.equal(db.getSync('counter'), counterIncrements * 2);
			db.close();
			result.conflicts = messages.reduce((sum, message) => sum + message.conflicts, 0);
		}
	}

	shutdown();
	console.log(JSON.stringify({ mode, ...result }));
}
