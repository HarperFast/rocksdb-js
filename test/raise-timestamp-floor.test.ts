import { raiseTimestampFloor } from '../src/index.ts';
import { dbRunner, generateDBPath, terminateWorker } from './lib/util.ts';
import { createWorkerBootstrapScript } from './lib/worker-bootstrap.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

// The floor is process-wide and shared with every test file running in this
// process, so in-process floors stay a few milliseconds ahead of the clock.
const NEAR_FUTURE_MS = 20;
const TEN_YEARS_MS = 10 * 365.25 * 24 * 3600 * 1000;

const fixture = join(
	dirname(fileURLToPath(import.meta.url)),
	'fixtures',
	'fork-raise-timestamp-floor.mts'
);

describe('raiseTimestampFloor', () => {
	it('issues every later timestamp strictly above the floor', () =>
		dbRunner(async ({ db }) => {
			const floor = db.getMonotonicTimestamp() + NEAR_FUTURE_MS;
			expect(raiseTimestampFloor(floor)).toBe(true);

			const clock = db.getMonotonicTimestamp();
			expect(clock).toBeGreaterThan(floor);
			await db.transaction((txn) => {
				expect(txn.getTimestamp()).toBeGreaterThan(clock);
			});
		}));

	it('is raise-only: a floor at or below the clock is a no-op', () =>
		dbRunner(async ({ db }) => {
			const current = db.getMonotonicTimestamp();
			expect(raiseTimestampFloor(current)).toBe(false);
			expect(raiseTimestampFloor(current - 60_000)).toBe(false);
			expect(db.getMonotonicTimestamp()).toBeGreaterThan(current);

			const floor = db.getMonotonicTimestamp() + NEAR_FUTURE_MS;
			expect(raiseTimestampFloor(floor)).toBe(true);
			expect(raiseTimestampFloor(floor)).toBe(false);
		}));

	it('leaves issued and adopted transaction timestamps alone', () =>
		dbRunner(async ({ db }) => {
			await db.transaction(async (txn) => {
				const before = txn.getTimestamp();
				expect(raiseTimestampFloor(db.getMonotonicTimestamp() + NEAR_FUTURE_MS)).toBe(true);
				expect(txn.getTimestamp()).toBe(before);

				txn.setTimestamp(before - 60_000);
				expect(txn.getTimestamp()).toBe(before - 60_000);

				const floor = db.getMonotonicTimestamp() + NEAR_FUTURE_MS;
				expect(raiseTimestampFloor(floor)).toBe(true);
				txn.setTimestamp();
				expect(txn.getTimestamp()).toBeGreaterThan(floor);
				await txn.put('k', 'v');
			});
		}));

	it('rejects a value the clock must never adopt', () => {
		for (const value of [Number.NaN, Infinity, -Infinity, 0, -0, -1]) {
			expect(() => raiseTimestampFloor(value), String(value)).toThrow(RangeError);
		}
		expect(() => raiseTimestampFloor(8.64e15)).toThrow(/largest valid Date/);
		expect(() => raiseTimestampFloor(Date.now() + 2 * TEN_YEARS_MS)).toThrow(
			/implausibly far ahead of the wall clock \(wall clock \d+\.\d{3}\)/
		);
		const untyped = raiseTimestampFloor as (...args: unknown[]) => boolean;
		expect(() => untyped()).toThrow(TypeError);
		for (const value of [
			undefined,
			null,
			true,
			'1764307857213',
			1764307857213n,
			Object(1764307857213),
			Symbol('floor'),
			{},
		]) {
			expect(() => untyped(value), String(value)).toThrow(TypeError);
		}
	});

	it('raises the clock for every worker in the process', () =>
		dbRunner(async ({ db }) => {
			const floor = db.getMonotonicTimestamp() + NEAR_FUTURE_MS;
			const worker = new Worker(
				createWorkerBootstrapScript('./test/workers/raise-timestamp-floor-worker.mts'),
				{ eval: true, workerData: { floor } }
			);
			try {
				const { raised } = await new Promise<{ raised: boolean }>((resolve, reject) => {
					worker.once('message', resolve);
					worker.once('error', reject);
					worker.once('exit', (code) => reject(new Error(`worker exited early (${code})`)));
				});
				expect(raised).toBe(true);
			} finally {
				await terminateWorker(worker);
			}
			expect(db.getMonotonicTimestamp()).toBeGreaterThan(floor);
		}));

	it('keys transaction-log batches above a floor ahead of the wall clock, after a restart', async () => {
		const dbPath = generateDBPath();
		const floor = Date.now() + 60 * 60 * 1000;
		try {
			const first = await runFixture('plain', dbPath, floor);
			expect(first.raised).toBe(true);
			expect(first.clock).toBeGreaterThan(floor);
			expect(first.txnTimestamp).toBeGreaterThan(first.clock);
			expect(first.logTimestamps).toEqual([first.txnTimestamp]);

			// The durable key from the first process sits above this restored
			// floor, so the named-log seed at open is what keeps the next key above it.
			const second = await runFixture('seeded', dbPath, floor - 60_000);
			expect(second.raised).toBe(true);
			expect(second.clock).toBeGreaterThan(first.txnTimestamp);
			expect(second.logTimestamps).toEqual([first.txnTimestamp, second.txnTimestamp]);
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(dbPath, { recursive: true, force: true });
			}
		}
	});
});

type FixtureResult = {
	raised: boolean;
	clock: number;
	txnTimestamp: number;
	logTimestamps: number[];
};

async function runFixture(mode: string, dbPath: string, floor: number): Promise<FixtureResult> {
	const { code, stdout, stderr } = await new Promise<{
		code: number | null;
		stdout: string;
		stderr: string;
	}>((resolve, reject) => {
		const child = spawn(process.execPath, [fixture, mode, dbPath, String(floor)], {
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => (stdout += chunk));
		child.stderr.on('data', (chunk) => (stderr += chunk));
		child.once('error', reject);
		child.once('close', (code) => resolve({ code, stdout, stderr }));
	});
	expect(code, stderr).toBe(0);
	return JSON.parse(stdout.trim().split('\n').at(-1)!);
}
