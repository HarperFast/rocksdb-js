import { registryStatus, RocksDatabase, Transaction } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { spawnSync } from 'node:child_process';
import { Worker } from 'node:worker_threads';

const [dbPath, mode = 'commit'] = process.argv.slice(2);

process.on('uncaughtException', (error) => {
	console.error(error);
	process.exit(1);
});
process.on('unhandledRejection', (error) => {
	console.error(error);
	process.exit(1);
});

if (!dbPath) {
	throw new Error('Usage: fork-legacy-commit-close.mts <dbPath> [commit|worker|probe]');
}

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) {
		throw new Error(`fixture assertion failed: ${message}`);
	}
}

if (mode === 'probe') {
	const db = RocksDatabase.open(dbPath);
	try {
		assert(db.getSync('committed') === 'value', 'independent opener did not see the commit');
	} finally {
		db.close();
	}
	process.exit(0);
}

function assertReopenable(): void {
	const status = registryStatus();
	assert(
		!status.some((entry) => entry.path === dbPath),
		`last-handle close left the descriptor in the registry after commit completion: ${JSON.stringify(status)}`
	);

	// A registry reopen could hand back the same leaked descriptor. A separate
	// process must acquire RocksDB's LOCK and read the committed value.
	const probe = spawnSync(process.execPath, [process.argv[1], dbPath, 'probe'], {
		encoding: 'utf8',
		timeout: 5000,
		env: {
			...process.env,
			ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '0',
		},
	});
	assert(
		probe.status === 0 && probe.signal === null,
		`independent reopen failed (status=${probe.status}, signal=${probe.signal}): ${probe.stderr}`
	);
}

const delayMs = Number(process.env.ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS ?? 0);
assert(delayMs > 5000, 'execute delay must exceed the transaction close drain');

if (mode === 'worker') {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/commit-last-handle-worker.mts'),
		{ eval: true, workerData: { dbPath } }
	);
	await new Promise<void>((resolve, reject) => {
		worker.once('message', (message: unknown) => {
			if (message === 'delayed') resolve();
			else reject(new Error(`unexpected worker message: ${message}`));
		});
		worker.once('error', reject);
	});
	await worker.terminate();
	assertReopenable();
	console.log(JSON.stringify({ ok: true }));
	process.exit(0);
}

const db = RocksDatabase.open(dbPath);
const txn = new Transaction(db.store);
txn.putSync('committed', 'value');
const commit = txn.commit();

const delayDeadline = Date.now() + 5000;
while (!isTransactionCommitExecuteDelayedForTesting()) {
	assert(Date.now() < delayDeadline, 'legacy commit did not reach the native execute delay');
	await new Promise((resolve) => setTimeout(resolve, 5));
}
db.close();
let commitError: unknown;
try {
	await commit;
} catch (error) {
	commitError = error;
}
assert(
	commitError instanceof Error && /Database not open/.test(commitError.message),
	`closed wrapper should only fail its aftercommit notification: ${commitError}`
);

assertReopenable();

console.log(JSON.stringify({ ok: true }));
