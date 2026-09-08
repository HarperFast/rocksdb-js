/**
 * Same-thread drop/commit ordering for the column-family commit gate
 * (HarperFast/rocksdb-js#806), run in a child so the commit execution mode
 * (ROCKSDB_JS_COMMIT_THREAD: legacy libuv / single lane / two-lane) and the
 * admitted-window seam (ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS) can be set in the
 * environment the process starts with.
 *
 * Round 1 — commit wins on the calling thread: an async commit is admitted
 * (observed through the gate counters, not assumed from a delay) and parked
 * inside its admitted window by the seam; the SAME thread then calls
 * dropSync(), which must wait for that commit to land rather than deadlock or
 * let RocksDB poison the database. Round 2 — drop wins: a commit staged before
 * the drop is refused with the contained error. Then the database must be
 * healthy: unrelated sync and transactional writes succeed, getLastError() is
 * null, and the dropped name can be recreated and written.
 *
 * Prints a JSON result on stdout; exits non-zero on any unexpected outcome.
 */
import { RocksDatabase } from '../../src/index.ts';
import { getCommitGateCountersForTesting } from '../../src/load-binding.ts';
import type { Transaction } from '../../src/transaction.ts';
import { mkdirSync } from 'node:fs';
import { setImmediate as macrotask } from 'node:timers/promises';

const [dbPath, mode] = process.argv.slice(2);
if (!dbPath) {
	console.error('Usage: fork-drop-commit-race.mts <dbPath> [optimistic|pessimistic]');
	process.exit(1);
}
const pessimistic = mode === 'pessimistic';
mkdirSync(dbPath, { recursive: true });

const result: Record<string, unknown> = { mode: pessimistic ? 'pessimistic' : 'optimistic' };

function spinUntilAdmitted(min: number): void {
	// Block the JS thread (no event-loop turn) until the commit lane has admitted
	// the commit: this is the window the same-thread dropSync() must respect.
	const cell = new Int32Array(new SharedArrayBuffer(4));
	const deadline = Date.now() + 10_000;
	while (getCommitGateCountersForTesting().commitsAdmitted < min) {
		if (Date.now() > deadline) {
			throw new Error('commit was never admitted');
		}
		Atomics.wait(cell, 0, 0, 1);
	}
}

function describeError(err: unknown): { code?: string; message: string } {
	const e = err as Error & { code?: string };
	return { code: e?.code, message: e?.message ?? String(err) };
}

const victim = RocksDatabase.open(dbPath, { name: 'victim', pessimistic });
let doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });

// Round 1: commit wins, drop waits (same thread).
const before = getCommitGateCountersForTesting();
const commit = doomed.transaction((txn: Transaction) => {
	txn.putSync('won', 1);
});
commit.catch(() => {});
// One macrotask lets db.transaction() reach the native commit call; the spin
// below then waits for the lane to admit it.
await macrotask();
spinUntilAdmitted(before.commitsAdmitted + 1);
const dropStart = Date.now();
doomed.dropSync();
result.dropWaitedMs = Date.now() - dropStart;
try {
	await commit;
	result.commitWins = 'fulfilled';
} catch (err) {
	result.commitWins = describeError(err);
}
const afterRound1 = getCommitGateCountersForTesting();
result.dropBeganAfterAdmission =
	afterRound1.dropsBegun === before.dropsBegun + 1 &&
	afterRound1.commitsAdmitted === before.commitsAdmitted + 1;

// Round 2: drop wins, the staged commit is refused.
doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
try {
	await doomed.transaction(async (txn: Transaction) => {
		await doomed.put('lost', 1, { transaction: txn });
		doomed.dropSync();
	});
	result.dropWins = 'fulfilled';
} catch (err) {
	result.dropWins = describeError(err);
}

// Health after both races.
victim.putSync('probe', 1);
await victim.transaction(async (txn: Transaction) => {
	await victim.put('txn-probe', 2, { transaction: txn });
});
result.victimProbe = victim.getSync('probe');
result.victimTxnProbe = victim.getSync('txn-probe');
result.lastError = victim.getLastError();
const fresh = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
fresh.putSync('fresh', 3);
result.freshProbe = fresh.getSync('fresh');
result.freshSeesOld = fresh.getSync('won') ?? fresh.getSync('lost') ?? null;
result.columns = fresh.columns;
fresh.close();
victim.close();

console.log(JSON.stringify(result));
