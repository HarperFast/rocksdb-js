import { steadyClockNow } from '../src/index.ts';
import { dbRunner, generateDBPath, terminateWorker } from './lib/util.ts';
import { createWorkerBootstrapScript } from './lib/worker-bootstrap.ts';
import { spawn } from 'node:child_process';
import { existsSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

type WorkerSamples = { first: number; second: number };

// Timers may fire slightly early; upper bounds use causal parent samples.
const STAGGER_MS = 50;
const WORKER_SLEEP_MS = 30;
const TOLERANCE_MS = 10;

function runWorker(sleepMs = WORKER_SLEEP_MS): { worker: Worker; samples: Promise<WorkerSamples> } {
	const worker = new Worker(createWorkerBootstrapScript('./test/workers/steady-clock-worker.mts'), {
		eval: true,
		workerData: { sleepMs },
	});
	const samples = new Promise<WorkerSamples>((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
		worker.once('exit', (code) => reject(new Error(`worker exited before sampling (${code})`)));
	});
	return { worker, samples };
}

describe('steadyClockNow', () => {
	it('returns a finite number and never decreases in a tight loop', () => {
		let previous = steadyClockNow();
		expect(Number.isFinite(previous)).toBe(true);
		for (let i = 0; i < 100_000; i++) {
			const next = steadyClockNow();
			expect(next).toBeGreaterThanOrEqual(previous);
			previous = next;
		}
	});

	it('measures elapsed time in milliseconds', async () => {
		const start = steadyClockNow();
		await delay(50);
		const elapsed = steadyClockNow() - start;
		expect(elapsed).toBeGreaterThanOrEqual(50 - TOLERANCE_MS);
	});

	it('shares one domain with staggered and restarted workers', async () => {
		// Three sequential workers: each starts after the parent has already
		// sampled and waited STAGGER_MS, so a worker-relative origin (which would
		// read near 0 in a fresh worker) fails the `before + stagger` bound, and
		// the worker's own two samples must show the sleep between them.
		for (let round = 0; round < 3; round++) {
			const before = steadyClockNow();
			await delay(STAGGER_MS);
			const { worker, samples } = runWorker();
			try {
				const { first, second } = await samples;
				const after = steadyClockNow();
				expect(first, `round ${round}`).toBeGreaterThanOrEqual(before + STAGGER_MS - TOLERANCE_MS);
				expect(second - first, `round ${round}`).toBeGreaterThanOrEqual(
					WORKER_SLEEP_MS - TOLERANCE_MS
				);
				expect(second, `round ${round}`).toBeLessThanOrEqual(after);
			} finally {
				await terminateWorker(worker);
			}
		}
	});

	it('brackets concurrent workers inside the parent samples', async () => {
		const before = steadyClockNow();
		await delay(STAGGER_MS);
		const runs = Array.from({ length: 4 }, () => runWorker());
		try {
			const results = await Promise.all(runs.map((run) => run.samples));
			const after = steadyClockNow();
			for (const [index, { first, second }] of results.entries()) {
				expect(first, `worker ${index}`).toBeGreaterThanOrEqual(before + STAGGER_MS - TOLERANCE_MS);
				expect(second, `worker ${index}`).toBeGreaterThanOrEqual(first);
				expect(second, `worker ${index}`).toBeLessThanOrEqual(after);
			}
		} finally {
			await Promise.all(runs.map((run) => terminateWorker(run.worker)));
		}
	});

	it('leaves getMonotonicTimestamp and transaction timestamps in the wall-clock domain', () =>
		dbRunner(async ({ db }) => {
			let previous = db.getMonotonicTimestamp();
			for (let i = 0; i < 1000; i++) {
				steadyClockNow();
				const next = db.getMonotonicTimestamp();
				expect(next).toBeGreaterThan(previous);
				previous = next;
			}
			const now = Date.now();
			expect(Math.abs(previous - now)).toBeLessThan(1000);

			await db.transaction((txn) => {
				steadyClockNow();
				const txnTimestamp = txn.getTimestamp();
				expect(txnTimestamp).toBeGreaterThan(previous);
				expect(Math.abs(txnTimestamp - Date.now())).toBeLessThan(1000);
			});
		}));

	// libfaketime intercepts native wall-clock reads in a child, leaving monotonic
	// clocks real. A Date.now stub alone would not exercise the native source.
	const faketimeLib = process.env.ROCKSDB_JS_FAKETIME_LIB;
	it.skipIf(
		process.platform !== 'linux' ||
			!!process.versions.bun ||
			!!process.versions.deno ||
			!faketimeLib
	)('keeps measuring elapsed time across backward and forward wall-clock steps', async () => {
		type Sample = { wall: number; monotonic: number; steady: number; sleepMs?: number };
		expect(existsSync(faketimeLib!)).toBe(true);
		const dbPath = generateDBPath();
		const timestampFile = `${dbPath}-faketime`;
		writeFileSync(timestampFile, '-1d\n');
		const child = spawn(
			process.execPath,
			[join(__dirname, 'fixtures', 'fork-steady-clock-faketime.mts'), dbPath],
			{
				env: {
					...process.env,
					LD_PRELOAD: faketimeLib,
					FAKETIME_TIMESTAMP_FILE: timestampFile,
					FAKETIME_NO_CACHE: '1',
					DONT_FAKE_MONOTONIC: '1',
					FAKETIME_DONT_FAKE_MONOTONIC: '1',
				},
				stdio: ['pipe', 'pipe', 'pipe'],
				timeout: 15000,
			}
		);
		const closed = new Promise<number | null>((resolve) => child.once('close', resolve));
		let spawnError: Error | undefined;
		child.once('error', (error) => (spawnError = error));
		let stderr = '';
		child.stderr.on('data', (chunk) => (stderr += chunk));
		try {
			const lines = createInterface({ input: child.stdout });
			const iterator = lines[Symbol.asyncIterator]();
			const readSample = async (): Promise<Sample> => {
				const { value, done } = await iterator.next();
				if (done) {
					throw new Error(`child exited early: ${spawnError ?? stderr}`);
				}
				return JSON.parse(value);
			};

			const first = await readSample();
			writeFileSync(timestampFile, '-2d\n');
			child.stdin.write('step\n');
			const second = await readSample();

			const dayMs = 24 * 3600 * 1000;
			expect(second.wall, 'wall clock stepped back').toBeLessThan(first.wall - dayMs / 2);
			expect(second.monotonic, 'ratchet still increases').toBeGreaterThan(first.monotonic);
			expect(second.monotonic - first.monotonic, 'ratchet stalled').toBeLessThan(1);
			expect(
				second.steady - first.steady,
				'steady clock measured the sleep'
			).toBeGreaterThanOrEqual(second.sleepMs! - TOLERANCE_MS);
			expect(second.steady - first.steady, 'steady clock did not follow the step').toBeLessThan(
				dayMs / 2
			);

			writeFileSync(timestampFile, '+1d\n');
			child.stdin.write('step\n');
			const third = await readSample();
			expect(third.wall - second.wall, 'wall clock stepped forward').toBeGreaterThan(dayMs);
			expect(
				third.monotonic - second.monotonic,
				'native wall clock followed forward step'
			).toBeGreaterThan(dayMs);
			expect(Math.abs(third.monotonic - third.wall)).toBeLessThan(1000);
			expect(third.steady - second.steady).toBeGreaterThanOrEqual(third.sleepMs! - TOLERANCE_MS);
			expect(third.steady - second.steady, 'steady clock did not follow forward step').toBeLessThan(
				dayMs / 2
			);
			child.stdin.end();
			const code = await closed;
			expect(code, stderr).toBe(0);
		} finally {
			child.kill();
			await closed;
			rmSync(timestampFile, { force: true });
			rmSync(dbPath, { force: true, recursive: true });
		}
	});

	// Vitest tests import `src`; this proves the built entry points ship the
	// export. Skipped when `dist` is not built (CI builds before testing).
	const distDir = join(__dirname, '..', 'dist');
	it.skipIf(!existsSync(join(distDir, 'index.mjs')) || !existsSync(join(distDir, 'index.cjs')))(
		'is exported from the built ESM and CJS entry points',
		async () => {
			const esm = await import(pathToFileURL(join(distDir, 'index.mjs')).href);
			expect(esm.steadyClockNow).toBeTypeOf('function');
			expect(esm.steadyClockNow()).toBeTypeOf('number');

			const cjs = createRequire(import.meta.url)(join(distDir, 'index.cjs'));
			expect(cjs.steadyClockNow).toBeTypeOf('function');
			expect(cjs.steadyClockNow()).toBeTypeOf('number');
		}
	);
});
