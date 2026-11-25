// commitThreads is a process-wide setting applied at open, so each case runs in a fresh process.
import { registryStatus, RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import assert from 'node:assert/strict';
import { stat } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';

const [path, mode, threadsArg] = process.argv.slice(2);

function commitThreadsStat(db: RocksDatabase): number {
	return db.getStat('commitPipeline.commitThreads') as number;
}

// Waits until `commit` holds the only commit thread in ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS, past its
// log stage.
async function untilCommitDelayed(commit: Promise<unknown>): Promise<void> {
	let settled = false;
	void commit.then(
		() => (settled = true),
		() => (settled = true)
	);
	while (!isTransactionCommitExecuteDelayedForTesting()) {
		assert.ok(!settled, 'the first commit settled before reaching its execute delay');
		await new Promise((resolve) => setImmediate(resolve));
	}
}

// Fails if `commit` has already settled.
async function assertPending(commit: Promise<unknown>): Promise<void> {
	const pending = Symbol('pending');
	assert.equal(
		await Promise.race([
			commit.then(
				() => 0,
				() => 0
			),
			pending,
		]),
		pending
	);
}

try {
	if (mode === 'validate') {
		for (const invalid of [0, 65, 1.5, Number.NaN, -1]) {
			assert.throws(() => RocksDatabase.config({ commitThreads: invalid }), RangeError);
		}
		assert.throws(() => RocksDatabase.config({ commitThreads: 'four' } as any), TypeError);
		RocksDatabase.config({ commitThreads: 64 });
		RocksDatabase.config({ commitThreads: 3 });
		const db = RocksDatabase.open(path);
		for (let i = 0; i < 20; i++) {
			const txn = new Transaction(db.store);
			txn.putSync(`k${i}`, i);
			await txn.commit();
		}
		// Commits awaited one at a time overlap only when one is enqueued between a thread delivering
		// the previous completion and going idle.
		assert.ok(commitThreadsStat(db) <= 2, `started ${commitThreadsStat(db)} threads`);
		await Promise.all(
			Array.from({ length: 50 }, (_, i) => {
				const txn = new Transaction(db.store);
				txn.putSync(`c${i}`, i);
				return txn.commit();
			})
		);
		assert.ok(commitThreadsStat(db) <= 3, `started ${commitThreadsStat(db)} threads`);
		// The limit is read when a database is first opened: another handle to the same path
		// shares its threads, and only a new path picks up a changed setting.
		RocksDatabase.config({ commitThreads: 1 });
		const sameDb = RocksDatabase.open(path);
		const otherDb = RocksDatabase.open(`${path}-other`);
		const burst = (target: RocksDatabase) =>
			Promise.all(
				Array.from({ length: 50 }, (_, i) => {
					const txn = new Transaction(target.store);
					txn.putSync(`b${i}`, i);
					return txn.commit();
				})
			);
		await Promise.all([burst(sameDb), burst(otherDb)]);
		assert.equal(commitThreadsStat(otherDb), 1);
		assert.equal(commitThreadsStat(sameDb), commitThreadsStat(db));
		otherDb.close();
		sameDb.close();
		db.close();
		console.log(JSON.stringify({ ok: true }));
	} else if (mode === 'concurrent') {
		// ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS stalls each RocksDB commit, so wall time shows how many
		// ran at once.
		RocksDatabase.config({ commitThreads: Number(threadsArg) });
		const db = RocksDatabase.open(path);
		// Leave one started thread idle, so the burst below must grow past a thread that is
		// still waking up.
		const warm = new Transaction(db.store);
		warm.putSync('warm', 0);
		await warm.commit();
		const started = performance.now();
		await Promise.all(
			Array.from({ length: 8 }, (_, i) => {
				const txn = new Transaction(db.store);
				txn.putSync(`k${i}`, i);
				return txn.commit();
			})
		);
		const wallMs = performance.now() - started;
		for (let i = 0; i < 8; i++) assert.equal(db.getSync(`k${i}`), i);
		const threads = commitThreadsStat(db);
		db.close();
		console.log(JSON.stringify({ ok: true, wallMs, threads }));
	} else if (mode === 'order') {
		// A large transaction dispatched first and a small one dispatched second: with more than one
		// commit thread the small one finishes first. Whenever a commit resolves, every transaction-log
		// entry visible to a committed read must belong to a transaction whose data is readable.
		RocksDatabase.config({ commitThreads: Number(threadsArg) });
		const db = RocksDatabase.open(path);
		const log = db.useLog('order');
		const resolved: string[] = [];
		const checkVisibleEntries = () => {
			for (const entry of log.query({ start: 0 })) {
				const name = entry.data.toString();
				assert.notEqual(
					db.getSync(`${name}-0`),
					undefined,
					`log entry for ${name} visible before its data`
				);
			}
		};
		const commit = (name: string, keys: number) => {
			const txn = new Transaction(db.store);
			for (let i = 0; i < keys; i++) txn.putSync(`${name}-${i}`, i);
			log.addEntry(Buffer.from(name), txn.id);
			return txn.commit().then(() => {
				resolved.push(name);
				checkVisibleEntries();
			});
		};
		await Promise.all([commit('big', 100_000), commit('small', 1)]);
		checkVisibleEntries();
		const names = [...log.query({ start: 0 })].map((entry) => entry.data.toString()).sort();
		assert.deepEqual(names, ['big', 'small']);
		db.close();
		console.log(JSON.stringify({ ok: true, resolved }));
	} else if (mode === 'starve') {
		// Commits stalled by ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS must not hold libuv threadpool slots.
		RocksDatabase.config({ commitThreads: Number(threadsArg) });
		const db = RocksDatabase.open(path);
		const commits = Array.from({ length: 8 }, (_, i) => {
			const txn = new Transaction(db.store);
			txn.putSync(`k${i}`, i);
			return txn.commit();
		});
		await new Promise((resolve) => setTimeout(resolve, 50));
		const started = performance.now();
		await stat(path);
		const statMs = performance.now() - started;
		await Promise.all(commits);
		db.close();
		console.log(JSON.stringify({ ok: true, statMs }));
	} else if (mode === 'close-queued') {
		// The first commit holds the only thread (commitThreads: 1, or UV_THREADPOOL_SIZE=1 for the
		// legacy path) in ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS, so the second is still queued when its
		// handle closes; another handle keeps the database open.
		RocksDatabase.config({ commitThreads: 1 });
		const db = RocksDatabase.open(path);
		const keeper = RocksDatabase.open(path);
		const first = new Transaction(keeper.store);
		first.putSync('first', 1);
		const firstCommit = first.commit();
		await untilCommitDelayed(firstCommit);
		const queued = new Transaction(db.store);
		queued.putSync('queued', 2);
		const queuedCommit = queued.commit();
		db.close();
		await assertPending(queuedCommit);
		await Promise.all([firstCommit, queuedCommit]);
		assert.equal(keeper.getSync('queued'), 2);
		keeper.close();
		console.log(JSON.stringify({ ok: true }));
	} else if (mode === 'close-queued-last') {
		// As close-queued, but the closing handle is the last one. A lane commit is refused: the close
		// runs while it is queued. A legacy commit pins the descriptor, which defers the close until
		// that commit completes, so it commits.
		const legacy = process.env.ROCKSDB_JS_COMMIT_THREAD === '0';
		RocksDatabase.config({ commitThreads: 1 });
		const db = RocksDatabase.open(path);
		const first = new Transaction(db.store);
		first.putSync('first', 1);
		const firstCommit = first.commit().catch((error) => {
			// The after-commit notification can find the database closed; the write is checked below.
			if (!/Database (not open|is closing)/.test(String(error))) throw error;
		});
		await untilCommitDelayed(firstCommit);
		const queued = new Transaction(db.store);
		queued.putSync('queued', 2);
		const queuedCommit = queued.commit();
		db.close();
		await assertPending(queuedCommit);
		await firstCommit;
		if (legacy) {
			await queuedCommit;
		} else {
			await assert.rejects(queuedCommit, /Database closed during transaction commit operation/);
		}
		assert.ok(
			!registryStatus().some((entry) => entry.path === path),
			'the last-handle close left the database open'
		);
		const reopened = RocksDatabase.open(path);
		assert.equal(reopened.getSync('first'), 1);
		assert.equal(reopened.getSync('queued'), legacy ? 2 : undefined);
		reopened.close();
		console.log(JSON.stringify({ ok: true }));
	} else {
		throw new Error(`unknown mode ${mode}`);
	}
} finally {
	shutdown();
}
