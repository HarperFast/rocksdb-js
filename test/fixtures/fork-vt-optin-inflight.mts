// Runs in its own process because ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS, which holds each commit
// just before RocksDB applies it, is read once at process start.
import { RocksDatabase, Transaction } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const [scenario, path] = process.argv.slice(2);
assert.ok(scenario && path);

const conflict = { code: 'ERR_VERIFICATION_TABLE_CONFLICT', message: /holding a write staged/ };
const keepAlive = RocksDatabase.open(path, { encoding: false });
const plain = RocksDatabase.open(path, { name: 'cached', encoding: false });
const cached = new RocksDatabase(path, {
	name: 'cached',
	encoding: false,
	verificationTable: true,
});

if (scenario === 'unrelated-commit') {
	// A commit in flight on another column family wrote nothing through the closed handle.
	plain.close();
	const txn = new Transaction(keepAlive.store);
	keepAlive.putSync(Buffer.from('unrelated'), Buffer.from('v'), { transaction: txn });
	const committing = txn.commit();
	cached.open();
	await committing;
} else if (scenario === 'worker-teardown') {
	// The worker's commit is still executing when its env is torn down, which removes the
	// transaction from the registry before close() waits that commit out.
	plain.close();
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/vt-optin-commit-worker.mts'),
		{
			eval: true,
			workerData: { path },
		}
	);
	await new Promise<void>((resolve, reject) => {
		worker.once('message', () => resolve());
		worker.once('error', reject);
	});
	assert.throws(() => cached.open(), conflict);
	const terminated = worker.terminate();
	await delay(200);
	assert.throws(() => cached.open(), conflict);
	await terminated;
	cached.open();
} else {
	throw new Error(`Unknown scenario ${scenario}`);
}

cached.close();
keepAlive.close();
console.log('ok');
