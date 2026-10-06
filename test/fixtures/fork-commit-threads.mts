// commitThreads is a process-wide setting applied at open, so each case runs in a fresh process.
import { RocksDatabase, shutdown, Transaction } from '../../src/index.ts';
import assert from 'node:assert/strict';
import { stat } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';

const [path, mode, threadsArg] = process.argv.slice(2);

function commitThreadsStat(db: RocksDatabase): number {
	return db.getStat('commitPipeline.commitThreads') as number;
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
	} else {
		throw new Error(`unknown mode ${mode}`);
	}
} finally {
	shutdown();
}
