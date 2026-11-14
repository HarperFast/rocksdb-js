import { ChildCloseTimeoutError, type ChildResult, runChild } from './lib/child-process.ts';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

// A real stall blocks the writer, so a parent process owns the deadline and kills the child.

const fixturePath = join(__dirname, 'fixtures', 'fork-wbm-stall-watchdog.mts');
const exitFixturePath = join(__dirname, 'fixtures', 'fork-wbm-watchdog-exit.mts');
const disabledFixturePath = join(__dirname, 'fixtures', 'fork-wbm-watchdog-disabled.mts');
const WARN_MS = 2000;

const childEnv = { ...process.env, ROCKSDB_JS_WBM_STALL_WARN_MS: String(WARN_MS) };

function runStallChild(
	dbPath: string,
	deadlineMs: number,
	shutdownWhenStalled = false
): Promise<ChildResult> {
	return runChild(
		process.execPath,
		[fixturePath, dbPath, ...(shutdownWhenStalled ? ['shutdown'] : [])],
		{
			deadlineMs,
			env: childEnv,
			stopWhen: ({ stdout, stderr }) =>
				shutdownWhenStalled
					? stderr.includes('WriteBufferManager write stall active for')
					: /^(STALLED|NEVER_STALLED|CLEARED)\r?$/m.test(stdout),
		}
	);
}

function runExitChild(dbPath: string, listenerOrder: 'before' | 'after'): Promise<ChildResult> {
	return runChild(process.execPath, [exitFixturePath, dbPath, listenerOrder], {
		deadlineMs: 15_000,
		env: childEnv,
	});
}

// A close timeout leaves the child potentially holding the directory open.
async function inTempDir(
	prefix: string,
	run: (dbPath: string) => Promise<ChildResult>
): Promise<ChildResult> {
	const dir = mkdtempSync(join(tmpdir(), prefix));
	let childMayBeRunning = false;
	try {
		return await run(join(dir, 'db'));
	} catch (error) {
		childMayBeRunning = error instanceof ChildCloseTimeoutError;
		throw error;
	} finally {
		if (!childMayBeRunning && !process.env.KEEP_FILES) {
			rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
		}
	}
}

describe('inTempDir()', () => {
	it('retains the directory when the child may still be running', async () => {
		let dir: string | undefined;
		const error = new ChildCloseTimeoutError('child did not close');
		vi.stubEnv('KEEP_FILES', '');
		try {
			await expect(
				inTempDir('rocksdb-wbm-close-error-', async (dbPath) => {
					dir = dirname(dbPath);
					throw error;
				})
			).rejects.toBe(error);
			expect(existsSync(dir!)).toBe(true);
		} finally {
			vi.unstubAllEnvs();
			if (dir) rmSync(dir, { recursive: true, force: true });
		}
	});

	it.each([false, true])('cleans up a failed spawn unless KEEP_FILES=%s', async (keepFiles) => {
		let dir: string | undefined;
		vi.stubEnv('KEEP_FILES', keepFiles ? '1' : '');
		try {
			await expect(
				inTempDir('rocksdb-wbm-spawn-error-', (dbPath) => {
					dir = dirname(dbPath);
					return runChild(join(dir, 'does-not-exist'), [], { deadlineMs: 100 });
				})
			).rejects.toMatchObject({ code: 'ENOENT' });
			expect(existsSync(dir!)).toBe(keepFiles);
		} finally {
			vi.unstubAllEnvs();
			if (dir) rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe('WriteBufferManager stall watchdog', () => {
	it.each(['before', 'after'] as const)(
		'exits cleanly when the global listener is registered %s watchdog construction',
		async (listenerOrder) => {
			const result = await inTempDir('rocksdb-wbm-exit-', (dbPath) =>
				runExitChild(dbPath, listenerOrder)
			);
			expect(result.timedOut, result.stderr).toBe(false);
			expect(result.signal, result.stderr).toBeNull();
			expect(result.code, result.stderr).toBe(0);
		}
	);

	it('starts no thread when the threshold is 0, with a stalling manager configured', () => {
		const dir = mkdtempSync(join(tmpdir(), 'rocksdb-wbm-disabled-'));
		try {
			const child = spawnSync(process.execPath, [disabledFixturePath, join(dir, 'db')], {
				encoding: 'utf8',
				timeout: 30000,
				env: { ...process.env, ROCKSDB_JS_WBM_STALL_WARN_MS: '0' },
			});
			expect(child.status, child.stderr).toBe(0);

			const stats = JSON.parse(child.stdout.trim().split(/\r?\n/).at(-1)!);
			expect(stats.enabled).toBe(true);
			expect(stats.allowStall).toBe(true);
			expect(stats.watchdogRunning).toBe(false);
			expect(stats.stallActiveMs).toBe(0);
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	});

	it('reports a sustained stall exactly once, and both read surfaces see it', async () => {
		const result = await inTempDir('rocksdb-wbm-stall-', (dbPath) =>
			runStallChild(dbPath, 120_000)
		);

		expect(result.timedOut, `child never finished:\n${result.stderr}`).toBe(false);
		expect(result.stdout.split(/\r?\n/), `child stderr:\n${result.stderr}`).toContain('STALLED');

		// Split on either line ending: the C++ warn line goes through the Windows
		// CRT's text-mode stderr, which turns its '\n' into '\r\n', while Node's
		// console.log on stdout does not translate.
		const warnings = result.stderr
			.split(/\r?\n/)
			.filter((line) => line.includes('WriteBufferManager write stall active for'));
		expect(warnings).toHaveLength(1);

		const [warning] = warnings;
		expect(warning).toMatch(/budget=\d/);
		expect(warning).toMatch(/usage=\d[\d.]*[KMGT]?B \(\d/);
		expect(warning).toMatch(/mutable=\d[\d.]*[KMGT]?B \(\d/);
		expect(warning).toContain('allowStall=true');
		expect(warning).toMatch(/columnFamilies=[1-9]\d*/);
		expect(warning).toMatch(/maxWriteBufferSizeToMaintain=\{33554432:[1-9]\d*\}/);

		const samples = result.stdout
			.split(/\r?\n/)
			.filter((line) => line.startsWith('STATS '))
			.map((line) => JSON.parse(line.slice('STATS '.length)));
		const stalled = samples.filter((sample) => sample.stats.stallActive);
		expect(stalled.length).toBeGreaterThan(0);

		const last = stalled.at(-1)!;
		expect(last.stats.enabled).toBe(true);
		expect(last.stats.allowStall).toBe(true);
		expect(last.stats.watchdogRunning).toBe(true);
		expect(last.stats.memoryUsage).toBeGreaterThanOrEqual(last.stats.bufferSize);
		expect(last.stats.mutableMemoryUsage).toBeLessThanOrEqual(last.stats.memoryUsage);
		expect(last.stats.stallActiveMs).toBeGreaterThanOrEqual(WARN_MS);
		expect(last.stats.columnFamilies).toBeGreaterThan(0);

		expect(last.getStats.stallActive).toBe(1);
		expect(last.getStats.bufferSize).toBe(last.stats.bufferSize);
		expect(last.getStat.stallActive).toBe(1);
		expect(last.getStat.bufferSize).toBe(last.stats.bufferSize);
		expect(last.getStats.stallActiveMs).toBeGreaterThanOrEqual(last.stats.stallActiveMs);
		expect(last.getStat.stallActiveMs).toBeGreaterThanOrEqual(last.getStats.stallActiveMs);

		const warned = result.stdout
			.split(/\r?\n/)
			.filter((line) => line.startsWith('WARNED '))
			.map((line) => line.slice('WARNED '.length));
		expect(warned).toHaveLength(1);
		expect(warned[0]).toBe(warning);

		expect(stalled.at(-1)!.stats.stallActiveMs).toBeGreaterThan(stalled[0].stats.stallActiveMs);
	}, 150_000);

	it('keeps reporting while shutdown waits for a stalled writer', async () => {
		const result = await inTempDir('rocksdb-wbm-stall-shutdown-', (dbPath) =>
			runStallChild(dbPath, 60_000, true)
		);

		expect(result.timedOut, `child never warned:\n${result.stderr}`).toBe(false);
		expect(result.stdout).toContain('SHUTTING_DOWN');
		expect(result.stderr).toContain('WriteBufferManager write stall active for');
	}, 90_000);
});
