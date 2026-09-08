import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// A real stall blocks the writer, so a parent process owns the deadline and kills the child.

const fixturePath = join(__dirname, 'fixtures', 'fork-wbm-stall-watchdog.mts');
const exitFixturePath = join(__dirname, 'fixtures', 'fork-wbm-watchdog-exit.mts');
const disabledFixturePath = join(__dirname, 'fixtures', 'fork-wbm-watchdog-disabled.mts');
const WARN_MS = 2000;

type ChildResult = { stdout: string; stderr: string; timedOut: boolean };

function runStallChild(
	dbPath: string,
	deadlineMs: number,
	shutdownWhenStalled = false
): Promise<ChildResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(
			process.execPath,
			[fixturePath, dbPath, ...(shutdownWhenStalled ? ['shutdown'] : [])],
			{
				env: { ...process.env, ROCKSDB_JS_WBM_STALL_WARN_MS: String(WARN_MS) },
			}
		);
		let stdout = '';
		let stderr = '';
		let timedOut = false;
		let settled = false;

		const finish = () => {
			if (settled) {
				return;
			}
			settled = true;
			clearTimeout(deadline);
			child.kill('SIGKILL');
			resolve({ stdout, stderr, timedOut });
		};

		const deadline = setTimeout(() => {
			timedOut = true;
			finish();
		}, deadlineMs);

		child.stdout?.on('data', (chunk) => {
			stdout += chunk.toString();
			if (!shutdownWhenStalled && /^(STALLED|NEVER_STALLED|CLEARED)\r?$/m.test(stdout)) {
				finish();
			}
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
			if (shutdownWhenStalled && stderr.includes('WriteBufferManager write stall active for')) {
				finish();
			}
		});
		child.on('error', (error) => {
			clearTimeout(deadline);
			reject(error);
		});
		child.on('close', finish);
	});
}

function runExitChild(
	dbPath: string,
	listenerOrder: 'before' | 'after'
): Promise<{
	code: number | null;
	signal: NodeJS.Signals | null;
	timedOut: boolean;
	stderr: string;
}> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [exitFixturePath, dbPath, listenerOrder], {
			env: { ...process.env, ROCKSDB_JS_WBM_STALL_WARN_MS: String(WARN_MS) },
		});
		let stderr = '';
		let timedOut = false;
		const deadline = setTimeout(() => {
			timedOut = true;
			child.kill('SIGKILL');
		}, 15_000);

		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('error', (error) => {
			clearTimeout(deadline);
			reject(error);
		});
		child.on('close', (code, signal) => {
			clearTimeout(deadline);
			resolve({ code, signal, timedOut, stderr });
		});
	});
}

describe('WriteBufferManager stall watchdog', () => {
	it.each(['before', 'after'] as const)(
		'exits cleanly when the global listener is registered %s watchdog construction',
		async (listenerOrder) => {
			const dir = mkdtempSync(join(tmpdir(), 'rocksdb-wbm-exit-'));
			try {
				const result = await runExitChild(join(dir, 'db'), listenerOrder);
				expect(result.timedOut, result.stderr).toBe(false);
				expect(result.signal, result.stderr).toBeNull();
				expect(result.code, result.stderr).toBe(0);
			} finally {
				if (!process.env.KEEP_FILES) {
					rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
				}
			}
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
		const dir = mkdtempSync(join(tmpdir(), 'rocksdb-wbm-stall-'));
		let result: ChildResult;
		try {
			result = await runStallChild(join(dir, 'db'), 120_000);
		} finally {
			if (!process.env.KEEP_FILES) {
				// The child was SIGKILLed a moment ago; on Windows its handles can
				// outlive it briefly, and an EPERM thrown here would mask the real
				// assertion result.
				rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
			}
		}

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
		const dir = mkdtempSync(join(tmpdir(), 'rocksdb-wbm-stall-shutdown-'));
		let result: ChildResult;
		try {
			result = await runStallChild(join(dir, 'db'), 60_000, true);
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
			}
		}

		expect(result.timedOut, `child never warned:\n${result.stderr}`).toBe(false);
		expect(result.stdout).toContain('SHUTTING_DOWN');
		expect(result.stderr).toContain('WriteBufferManager write stall active for');
	}, 90_000);
});
