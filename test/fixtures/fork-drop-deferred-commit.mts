import { RocksDatabase } from '../../src/index.ts';
import { forceDropFailureForTesting } from '../../src/load-binding.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { rmSync, writeSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

// Child-process scenarios for a drop racing a commit that is already admitted
// (AGENTS.md invariant 22). Run with ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS set so
// the worker's commit parks after admission, before `txn->Commit()`, and with
// ROCKSDB_JS_COMMIT_THREAD selecting the commit execution mode. Both are read
// once per process through ::getenv, which is why this is a child process and
// not a Vitest worker.
//
//   argv: <dbPath> <scenario> <optimistic|pessimistic> <sync|async>
//   scenario: admitted-commit | worker-terminated | open-waits | crash-reopen | retry-race
//
// Prints one JSON line on success; a failed assertion exits non-zero.
// `crash-reopen` SIGKILLs itself inside the deferral window and leaves the
// database for the parent to reopen; `retry-race` exercises a concurrent
// open() and drop() retry of a failed physical drop and needs no delay seam.

const [dbPath, scenario, txnMode, dropKind] = process.argv.slice(2);
const pessimistic = txnMode === 'pessimistic';
const delayMs = Number(process.env.ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS ?? 0);
if (!(delayMs >= 100)) {
	throw new Error('ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS must be >= 100 for this fixture');
}

// A Windows drive-letter path is not a valid Node ESM import specifier.
const workerPath = './test/workers/drop-deferred-commit-worker.mts';

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) {
		throw new Error(`fixture assertion failed: ${message}`);
	}
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function pendingReclaims(
	db: RocksDatabase,
	expected: number,
	timeoutMs = 5000
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const pending = db.getStat('columnFamily.pendingReclaims');
		if (pending === expected) return;
		assert(Date.now() < deadline, `pendingReclaims=${pending}, expected ${expected}`);
		await sleep(10);
	}
}

const meta = RocksDatabase.open(dbPath, { name: 'meta', pessimistic });
const table = RocksDatabase.open(dbPath, { name: 'table', pessimistic });
table.putSync('seed', 'old-generation');

const worker = new Worker(createWorkerBootstrapScript(workerPath), {
	eval: true,
	workerData: { path: dbPath, name: 'table', pessimistic },
});
const messages: Record<string, unknown>[] = [];
const waiters: Array<(m: Record<string, unknown>) => void> = [];
worker.on('message', (m: Record<string, unknown>) => {
	messages.push(m);
	waiters.splice(0).forEach((w) => w(m));
});
const nextMessage = (key: string): Promise<Record<string, unknown>> =>
	new Promise((resolve) => {
		const existing = messages.find((m) => key in m);
		if (existing) {
			messages.splice(messages.indexOf(existing), 1);
			resolve(existing);
			return;
		}
		const wait = (m: Record<string, unknown>) => {
			if (key in m) {
				messages.splice(messages.indexOf(m), 1);
				resolve(m);
			} else {
				waiters.push(wait);
			}
		};
		waiters.push(wait);
	});

const drop = async (): Promise<number> => {
	const started = Date.now();
	if (dropKind === 'async') {
		await table.drop();
	} else {
		table.dropSync();
	}
	return Date.now() - started;
};

if (scenario === 'retry-race') {
	await nextMessage('ready');
	for (let round = 0; round < 10; round++) {
		const generation = RocksDatabase.open(dbPath, { name: 'table', pessimistic });
		generation.putSync('seed', `generation-${round}`);
		forceDropFailureForTesting(1);
		let failed = false;
		try {
			generation.dropSync();
		} catch {
			failed = true;
		}
		forceDropFailureForTesting(0);
		assert(failed, 'forced drop failure did not surface');
		assert(meta.getStat('columnFamily.pendingReclaims') === 1, 'failed drop should stay pending');

		// Both sides retry the same failed generation at once: the open must
		// either perform the retry or wait for it, never report a failure that
		// did not happen. The worker parks on the barrier and opens the instant
		// it is released, so the two retries overlap as tightly as two threads
		// can be made to.
		const barrier = new Int32Array(new SharedArrayBuffer(4));
		worker.postMessage({ open: true, barrier });
		await sleep(20);
		Atomics.store(barrier, 0, 1);
		Atomics.notify(barrier, 0);
		generation.dropSync();
		const opened = await nextMessage('opened');
		assert(opened.error === undefined, `concurrent open failed: ${opened.error}`);
		assert(opened.seed === undefined, 'fresh family must not see the retired generation');
		assert(meta.getStat('columnFamily.pendingReclaims') === 0, 'generation should be reclaimed');
		generation.close();
	}
	worker.postMessage({ close: true });
	await nextMessage('closed');
	await worker.terminate();
	console.log(JSON.stringify({ scenario, txnMode, dropKind, ok: true }));
	table.close();
	meta.close();
	rmSync(dbPath, { recursive: true, force: true });
	process.exit(0);
}

try {
	await nextMessage('ready');
	worker.postMessage({ commit: true });
	await nextMessage('committing');
	// The commit is now admitted and parked in the execute-delay seam.
	await sleep(Math.min(50, delayMs / 4));

	const dropElapsed = await drop();
	assert(dropElapsed < delayMs / 2, `drop blocked for ${dropElapsed}ms behind the admitted commit`);
	assert(!table.columns.includes('table'), 'name still listed after logical drop');
	assert(
		meta.getStat('columnFamily.pendingReclaims') === 1,
		'the admitted commit should defer the physical drop'
	);

	// The environment is healthy while the drop is deferred, and the retired
	// generation is never mutated through a retained handle.
	meta.putSync('probe-during', 1);
	assert(meta.getLastError() === null, 'background error latched during deferral');
	table.clearSync();
	table.putSync('late', 'discarded');
	assert(
		table.getSync('seed') === 'old-generation',
		'clear on a retired generation must be a no-op'
	);
	assert(table.getSync('late') === undefined, 'write to a retired generation must be discarded');

	if (scenario === 'crash-reopen') {
		// Die inside the window: the parent reopens the database and finds the
		// retired generation on disk under its name (documented, not solved).
		writeSync(1, 'ready\n');
		process.kill(process.pid, 'SIGKILL');
	}

	if (scenario === 'worker-terminated') {
		// The worker dies with its commit admitted; the lane task still owns the
		// transaction handle, finishes, and releases the claim.
		await worker.terminate();
	} else if (scenario === 'open-waits') {
		const started = Date.now();
		const fresh = RocksDatabase.open(dbPath, { name: 'table', pessimistic });
		const elapsed = Date.now() - started;
		try {
			assert(
				elapsed >= delayMs / 4,
				`open returned after ${elapsed}ms without waiting for the admitted commit`
			);
			assert(fresh.getSync('seed') === undefined, 'fresh family must not see old data');
			assert(
				fresh.getSync('committed-before-drop') === undefined,
				'fresh family must not see the deferred commit'
			);
			fresh.putSync('new', 'generation');
			assert(fresh.getSync('new') === 'generation', 'fresh family must be writable');
		} finally {
			fresh.close();
		}
		const done = await nextMessage('done');
		assert(done.error === undefined, `admitted commit should have succeeded: ${done.error}`);
	} else {
		const done = await nextMessage('done');
		assert(done.error === undefined, `admitted commit should have succeeded: ${done.error}`);
		assert(done.lastError === null, 'worker saw a latched background error');
	}

	await pendingReclaims(meta, 0);
	meta.putSync('probe-after', 2);
	assert(meta.getSync('probe-after') === 2, 'unrelated family must keep writing');
	assert(meta.getLastError() === null, 'background error latched after reclaim');

	if (scenario !== 'open-waits') {
		const fresh = RocksDatabase.open(dbPath, { name: 'table', pessimistic });
		try {
			assert(fresh.getSync('seed') === undefined, 'recreated family must be empty');
			assert(
				fresh.getSync('committed-before-drop') === undefined,
				'recreated family must not see the deferred commit'
			);
			fresh.putSync('new', 'generation');
			assert(fresh.getSync('new') === 'generation', 'recreated family must be writable');
		} finally {
			fresh.close();
		}
	}

	if (scenario !== 'worker-terminated') {
		worker.postMessage({ close: true });
		await nextMessage('closed');
		await worker.terminate();
	}
	console.log(JSON.stringify({ scenario, txnMode, dropKind, dropElapsed, ok: true }));
} finally {
	table.close();
	meta.close();
	rmSync(dbPath, { recursive: true, force: true });
}
