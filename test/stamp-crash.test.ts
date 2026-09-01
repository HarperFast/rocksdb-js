import { RocksDatabase, validateTransactionLogStore, type Transaction } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * Commit-stamping fault-injection matrix
 * (docs/design/local-mutation-stamping.md §5): a child process armed with
 * ROCKSDB_JS_CRASH_POINT dies hard (exit 137) at each stamping/WAL boundary;
 * the parent reopens the store and asserts that no record-word/batch-key stamp
 * divergence survived, the recovered floor never re-mints a durable stamp, and
 * the transaction log validates.
 *
 * Node-only (the fixture and seams are exercised through spawned children).
 */

const fixturePath = join(import.meta.dirname, 'fixtures', 'fork-stamp-crash.mts');
const isNode = !process.versions.bun && !process.versions.deno;

const paths: string[] = [];

function dbPath(): string {
	const path = generateDBPath();
	paths.push(path);
	return path;
}

afterEach(() => {
	for (const path of paths.splice(0)) {
		rmSync(path, { recursive: true, force: true });
		rmSync(`${path}-logs`, { recursive: true, force: true });
	}
});

interface ChildResult {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runChild(path: string, mode: string, crashPoint?: string): Promise<ChildResult> {
	return new Promise((resolve, reject) => {
		const env: NodeJS.ProcessEnv = { ...process.env };
		delete env.ROCKSDB_JS_CRASH_POINT;
		if (crashPoint) {
			env.ROCKSDB_JS_CRASH_POINT = crashPoint;
		}
		const child = spawn(process.execPath, [fixturePath, path, mode], { env });
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => (stdout += chunk.toString()));
		child.stderr.on('data', (chunk) => (stderr += chunk.toString()));
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stdout, stderr }));
	});
}

/** Unarmed baseline run, then the armed crash run; returns the baseline stamp + crash exit code. */
async function baselineThenCrash(
	path: string,
	mode: 'crash' | 'crash-large',
	crashPoint: string
): Promise<{ baselineStamp: number; code: number | null; stderr: string }> {
	const baseline = await runChild(path, 'baseline');
	expect(baseline.code).toBe(0);
	const line = baseline.stdout.split('\n').find((l) => l.startsWith('{'));
	expect(line, `baseline output missing (stderr=${baseline.stderr})`).toBeDefined();
	const { baselineStamp } = JSON.parse(line!);
	const crash = await runChild(path, mode, crashPoint);
	return { baselineStamp, code: crash.code, stderr: crash.stderr };
}

interface RecoveredState {
	baseline: Buffer | undefined;
	crashRecord: Buffer | undefined;
	entries: { timestamp: number; data: string }[];
	nextStamp: number;
}

function reopenAndInspect(path: string): RecoveredState {
	const db = RocksDatabase.open(path, {
		encoding: 'binary',
		transactionLogsPath: `${path}-logs`,
	});
	try {
		const log = db.useLog('audit');
		const entries = [...log.query({ start: 1 })].map((entry) => ({
			timestamp: entry.timestamp,
			data: Buffer.from(entry.data).toString(),
		}));
		const baseline = db.getSync('baseline') as Buffer | undefined;
		const crashRecord = db.getSync('crash') as Buffer | undefined;
		// The marker inherits stamping; a fresh commit proves the recovered floor
		// exceeds every durable stamp.
		const txn = db.transactionSync((t: Transaction): Transaction => {
			t.putSync('post-recovery', Buffer.alloc(16));
			return t;
		}) as Transaction;
		return { baseline, crashRecord, entries, nextStamp: txn.getCommittedLocalTime()! };
	} finally {
		db.close();
	}
}

async function validateLogs(path: string): Promise<void> {
	const result = await validateTransactionLogStore(join(`${path}-logs`, 'audit'));
	expect(result.errors).toEqual([]);
}

describe.skipIf(!isNode)('commit stamping crash matrix', () => {
	it('control: unarmed children commit cleanly', async () => {
		const path = dbPath();
		expect((await runChild(path, 'baseline')).code).toBe(0);
		expect((await runChild(path, 'crash')).code).toBe(0);
		const state = reopenAndInspect(path);
		expect(state.crashRecord).toBeDefined();
		expect(state.entries.length).toBe(2);
	});

	it('F1: crash before the log write leaves nothing visible', async () => {
		const path = dbPath();
		const run = await baselineThenCrash(path, 'crash', 'stamp-before-log-write');
		expect(run.code, run.stderr).toBe(137);
		const state = reopenAndInspect(path);
		expect(state.crashRecord).toBeUndefined();
		expect(state.entries.map((entry) => entry.data)).toEqual(['baseline-entry']);
		expect(state.nextStamp).toBeGreaterThan(run.baselineStamp);
		await validateLogs(path);
	});

	it('F2: crash mid-append leaves a torn tail that recovery discards', async () => {
		const path = dbPath();
		const run = await baselineThenCrash(path, 'crash-large', 'mid-log-append');
		expect(run.code, run.stderr).toBe(137);
		const state = reopenAndInspect(path);
		// The batch was incomplete (no last-entry flag landed on the final
		// entry), so recovery must not expose any prefix of it and the crash
		// record never committed.
		expect(state.crashRecord).toBeUndefined();
		expect(state.entries.map((entry) => entry.data)).toEqual(['baseline-entry']);
		expect(state.nextStamp).toBeGreaterThan(run.baselineStamp);
		await validateLogs(path);
	});

	it('F3: crash after the log write, before the RocksDB commit', async () => {
		const path = dbPath();
		const run = await baselineThenCrash(path, 'crash', 'after-log-write');
		expect(run.code, run.stderr).toBe(137);
		const state = reopenAndInspect(path);
		// The batch is durable at its claimed stamp key; the record never
		// committed (replay healing is the caller's job — harper#2411), and the
		// recovered floor still exceeds the batch stamp so it can never be
		// re-minted.
		expect(state.crashRecord).toBeUndefined();
		const crashEntry = state.entries.find((entry) => entry.data === 'crash-entry-0');
		expect(crashEntry).toBeDefined();
		expect(crashEntry!.timestamp).toBeGreaterThan(run.baselineStamp);
		expect(state.nextStamp).toBeGreaterThan(crashEntry!.timestamp);
		await validateLogs(path);
	});

	it('F4: crash after the RocksDB commit, before commitFinished', async () => {
		const path = dbPath();
		const run = await baselineThenCrash(path, 'crash', 'after-rocksdb-commit');
		expect(run.code, run.stderr).toBe(137);
		const state = reopenAndInspect(path);
		// Both sides durable: the record's first word must equal the batch key
		// exactly (the atomicity contract), and the floor exceeds it.
		expect(state.crashRecord).toBeDefined();
		const stamp = state.crashRecord!.readDoubleBE(0);
		const crashEntry = state.entries.find((entry) => entry.data === 'crash-entry-0');
		expect(crashEntry).toBeDefined();
		expect(crashEntry!.timestamp).toBe(stamp);
		expect(state.nextStamp).toBeGreaterThan(stamp);
		await validateLogs(path);
	});

	it('F5: crash after commitFinished', async () => {
		const path = dbPath();
		const run = await baselineThenCrash(path, 'crash', 'after-commit-finished');
		expect(run.code, run.stderr).toBe(137);
		const state = reopenAndInspect(path);
		expect(state.crashRecord).toBeDefined();
		const stamp = state.crashRecord!.readDoubleBE(0);
		const crashEntry = state.entries.find((entry) => entry.data === 'crash-entry-0');
		expect(crashEntry!.timestamp).toBe(stamp);
		expect(state.nextStamp).toBeGreaterThan(stamp);
		await validateLogs(path);
	});

	it('F8: crash between the reserve extension and its publication', async () => {
		const path = dbPath();
		// The enabling open performs the open-time reserve extension; armed, the
		// child dies right after the ceiling row is durable but before it is
		// published to claims. Either ceiling (old or new) must recover to a
		// value >= every issued stamp — none were issued here — and the store
		// must open and stamp cleanly afterwards.
		expect((await runChild(path, 'baseline')).code).toBe(0);
		const crashed = await runChild(path, 'reserve-crash', 'after-reserve-extend');
		expect(crashed.code, crashed.stderr).toBe(137);
		const state = reopenAndInspect(path);
		expect(state.nextStamp).toBeGreaterThan(0);
	});
});
