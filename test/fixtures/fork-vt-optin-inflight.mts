// Runs in its own process because ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS, which holds each commit
// just before RocksDB applies it, is read once at process start.
import { registryStatus, RocksDatabase, Transaction } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

const [scenario, path] = process.argv.slice(2);
assert.ok(scenario && path);

async function waitFor(condition: () => boolean, what: string): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (!condition()) {
		assert.ok(Date.now() < deadline, `timed out waiting for ${what}`);
		await delay(5);
	}
}

const pendingTransactions = () => registryStatus().find((db) => db.path === path)?.transactions;
const conflict = { code: 'ERR_VERIFICATION_TABLE_CONFLICT', message: /holding a write staged/ };
const keepAlive = RocksDatabase.open(path, { encoding: false });
const plain = RocksDatabase.open(path, { name: 'cached', encoding: false });
const cached = new RocksDatabase(path, {
	name: 'cached',
	encoding: false,
	verificationTable: true,
});

if (scenario === 'unrelated-commit') {
	plain.close();
	const txn = new Transaction(keepAlive.store);
	keepAlive.putSync(Buffer.from('unrelated'), Buffer.from('v'), { transaction: txn });
	const committing = txn.commit();
	await waitFor(isTransactionCommitExecuteDelayedForTesting, 'the commit to execute');
	cached.open();
	await committing;
} else if (scenario === 'worker-teardown') {
	// Env teardown removes the transaction from the registry, then waits out its commit.
	plain.close();
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/vt-optin-commit-worker.mts'),
		{ eval: true, workerData: { path } }
	);
	await new Promise<void>((resolve, reject) => {
		worker.once('message', () => resolve());
		worker.once('error', reject);
	});
	await waitFor(isTransactionCommitExecuteDelayedForTesting, 'the commit to execute');
	assert.throws(() => cached.open(), conflict);
	const terminated = worker.terminate();
	// A legacy (libuv) commit finishes before env cleanup reaps its transaction.
	if (!['0', 'false'].includes(process.env.ROCKSDB_JS_COMMIT_THREAD ?? '')) {
		await waitFor(() => pendingTransactions() === 0, 'teardown to remove the transaction');
	}
	assert.throws(() => cached.open(), conflict);
	assert.ok(isTransactionCommitExecuteDelayedForTesting(), 'the commit finished before the check');
	await terminated;
	cached.open();
} else {
	throw new Error(`Unknown scenario ${scenario}`);
}

cached.close();
keepAlive.close();
console.log('ok');
