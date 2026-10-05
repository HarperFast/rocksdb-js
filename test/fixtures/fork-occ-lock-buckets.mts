import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { TransactionIsBusyError } from '../../src/transaction.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
// The pool freezes process-wide, so each configuration needs a fresh process.
import assert from 'node:assert/strict';
import { mkdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const options = { noBlockCache: true, writeBufferSize: 1024 * 1024, parallelismThreads: 1 };
const frozenError = /occLockBuckets cannot be changed after the shared pool has been created/;

function checkFrozen(count: number): void {
	RocksDatabase.config({ occLockBuckets: count });
	RocksDatabase.config({ occLockBuckets: undefined });
	RocksDatabase.config({ occLockBuckets: null } as any);
	assert.throws(() => RocksDatabase.config({ occLockBuckets: count * 2 }), frozenError);
}

function anonymousMiB(): number | null {
	if (process.platform !== 'linux') return null;
	return (
		Number(/Anonymous:\s+(\d+)/.exec(readFileSync('/proc/self/smaps_rollup', 'utf8'))![1]) / 1024
	);
}

if (!isMainThread) {
	checkFrozen(16);
	parentPort!.postMessage('ready');
	await new Promise<void>((resolve) => parentPort!.once('message', resolve));
	for (let round = 0; round < 8; round++) {
		const db = RocksDatabase.open(workerData.path, options);
		try {
			for (let batch = 0; batch < 8; batch++) {
				const txn = new Transaction(db.store);
				for (let key = 0; key < 64; key++) txn.putSync(`key-${key}`, round * 8 + batch);
				if (batch % 2) txn.commitSync();
				else await txn.commit();
			}
			for (let key = 0; key < 64; key++) assert.equal(db.getSync(`key-${key}`), round * 8 + 7);
		} finally {
			db.close();
		}
	}
	parentPort!.close();
} else {
	const [root, mode] = process.argv.slice(2);
	assert.ok(root);
	mkdirSync(root, { recursive: true });
	assert.ok(['default', 'configured', 'transactions'].includes(mode));
	const count = mode === 'default' ? 1 << 20 : mode === 'configured' ? 16384 : 16;

	if (mode === 'transactions') {
		for (const invalid of [0, -1, 15, 1.5, NaN, Infinity, -Infinity, 16777217, 2 ** 32]) {
			assert.throws(() => RocksDatabase.config({ occLockBuckets: invalid }), RangeError);
		}
		for (const invalid of ['16', true, {}, 16n]) {
			assert.throws(() => RocksDatabase.config({ occLockBuckets: invalid } as any), TypeError);
		}
		RocksDatabase.config({ occLockBuckets: 16777216 });
		RocksDatabase.config({ occLockBuckets: 17 });
		const pessimisticPath = join(root, 'pessimistic');
		const pessimistic = RocksDatabase.open(pessimisticPath, { ...options, pessimistic: true });
		pessimistic.putSync('key', 'value');
		pessimistic.close();
		RocksDatabase.config({ occLockBuckets: 32 });
		const readOnly = RocksDatabase.open(pessimisticPath, { ...options, readOnly: true });
		assert.equal(readOnly.getSync('key'), 'value');
		readOnly.close();
		RocksDatabase.config({ occLockBuckets: 17 });
	}
	if (mode !== 'default') RocksDatabase.config({ occLockBuckets: count });

	const before = anonymousMiB();
	const dbs = Array.from({ length: 10 }, (_, i) =>
		RocksDatabase.open(join(root, `db${i}`), options)
	);
	const after = anonymousMiB();
	const growth = before === null || after === null ? null : after - before;
	try {
		if (mode !== 'default') checkFrozen(count);
		if (mode === 'transactions') {
			for (const sync of [false, true]) {
				const txn = new Transaction(dbs[0].store);
				for (let i = 0; i < 1024; i++) txn.putSync(`bulk-${sync}-${i}`, i);
				if (sync) txn.commitSync();
				else await txn.commit();
				for (let i = 0; i < 1024; i++) assert.equal(dbs[0].getSync(`bulk-${sync}-${i}`), i);
			}
			const other = RocksDatabase.open(join(root, 'db0'), { ...options, name: 'other' });
			try {
				const txns = [dbs[0], dbs[1], other].map((db, i) => {
					const txn = new Transaction(db.store);
					txn.putSync('same-key', i);
					return txn;
				});
				await Promise.all(txns.map((txn) => txn.commit()));
				[dbs[0], dbs[1], other].forEach((db, i) => assert.equal(db.getSync('same-key'), i));
			} finally {
				other.close();
			}
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
			if (!process.versions.deno) {
				const workers = [0, 1].map(
					(i) =>
						new Worker(createWorkerBootstrapScript(fileURLToPath(import.meta.url)), {
							eval: true,
							workerData: { path: join(root, `worker${i}`) },
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
								worker.once('message', resolve);
							})
					)
				);
				for (const worker of workers) worker.postMessage('start');
				await Promise.all(exited);
			}
		}
	} finally {
		for (const db of dbs.reverse()) db.close();
	}
	shutdown();
	if (mode !== 'default') checkFrozen(count);
	const reopened = RocksDatabase.open(join(root, 'db0'), options);
	reopened.putSync('reopened', true);
	assert.equal(reopened.getSync('reopened'), true);
	reopened.close();
	console.log(JSON.stringify({ mode, anonymousMiB: growth }));
}
