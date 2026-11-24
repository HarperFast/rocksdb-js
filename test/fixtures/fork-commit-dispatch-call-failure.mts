// Commits whose completions cannot be handed to their live env must still settle, and must not
// keep the event loop alive: the parent kills this process on a deadline.
import { RocksDatabase, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';

const [path, mode] = process.argv.slice(2);
const failures = Number(process.env.ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE);
const skips = Number(process.env.ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE_SKIP ?? 0);
const expected = { burst: [2, 0], 'delivered-first': [1, 1], 'worker-exit': [1, 0] }[mode];
if (!expected || failures !== expected[0] || skips !== expected[1]) {
	throw new Error(`unexpected mode ${mode} or seams ${failures}/${skips}`);
}

if (mode === 'worker-exit') {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/commit-dispatch-call-failure-worker.mts'),
		{ eval: true, workerData: { path } }
	);
	const exitCode = await new Promise<number>((resolve, reject) => {
		worker.once('error', reject);
		worker.once('exit', resolve);
	});
	assert.equal(exitCode, 0);
	const db = RocksDatabase.open(path);
	assert.equal(db.getSync('worker'), 1);
	db.close();
	console.log(JSON.stringify({ ok: true }));
	process.exitCode = 0;
} else {
	const db = RocksDatabase.open(path);
	const commit = (key: string, value: number) => {
		const txn = new Transaction(db.store);
		txn.putSync(key, value);
		return txn.commit();
	};

	const burst = Array.from({ length: 8 }, (_, i) => commit(`burst${i}`, i));
	if (mode === 'delivered-first') {
		// Holds the JS thread so the delivered completion is still queued when the next call fails,
		// leaving its finish() to release the retired completion.
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);
	}
	await Promise.all(burst);
	if (mode === 'burst') {
		// The replacement completion fails once more, alone.
		await commit('alone', 1);
		assert.equal(db.getSync('alone'), 1);
	}
	await commit('after', 2);

	for (let i = 0; i < 8; i++) {
		assert.equal(db.getSync(`burst${i}`), i);
	}
	assert.equal(db.getSync('after'), 2);
	db.close();
	console.log(JSON.stringify({ ok: true }));
}
