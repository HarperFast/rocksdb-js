import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * The WriteBufferManager stall watchdog, against a real stall.
 *
 * Two constraints force the child-process shape. `allowStall` is fixed when the
 * manager singleton is constructed, so this needs a process no other manager has
 * run in; and a real stall blocks the thread that writes, so the runner's own
 * timeout cannot fire (HarperFast/rocksdb-js#781 item 2) — the deadline has to be
 * the parent's, and the child is killed rather than asked to exit.
 *
 * The decision logic itself is unit-tested deterministically in
 * `test/native/wbm_stall_watchdog_test.cc`; this proves the wiring: that a stall
 * is reached, that both read surfaces see it, and that the watchdog writes exactly
 * one line no matter how long it lasts.
 */

const fixturePath = join(__dirname, 'fixtures', 'fork-wbm-stall-watchdog.mts');
const WARN_MS = 2000;

type ChildResult = { stdout: string; stderr: string; timedOut: boolean };

function runStallChild(dbPath: string, deadlineMs: number): Promise<ChildResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, dbPath], {
			env: { ...process.env, ROCKSDB_JS_WBM_STALL_WARN_MS: String(WARN_MS) },
		});
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
			if (/^(STALLED|NEVER_STALLED|CLEARED)\r?$/m.test(stdout)) {
				finish();
			}
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('error', (error) => {
			clearTimeout(deadline);
			reject(error);
		});
		child.on('close', finish);
	});
}

describe('WriteBufferManager stall watchdog', () => {
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
		// Anchored: 'NEVER_STALLED' also contains 'STALLED', and a run that never
		// reached a stall must fail here rather than in the warn-count assertion.
		expect(result.stdout.split(/\r?\n/), `child stderr:\n${result.stderr}`).toContain('STALLED');

		// Split on either line ending: the C++ warn line goes through the Windows
		// CRT's text-mode stderr, which turns its '\n' into '\r\n', while Node's
		// console.log on stdout does not translate.
		const warnings = result.stderr
			.split(/\r?\n/)
			.filter((line) => line.includes('WriteBufferManager write stall active for'));
		// Not one per blocked writer and not one per poll: the stall is held for
		// several times the threshold and still yields a single line.
		expect(warnings).toHaveLength(1);

		const [warning] = warnings;
		expect(warning).toMatch(/budget=\d/);
		expect(warning).toMatch(/usage=\d[\d.]*[KMGT]?B \(\d/);
		expect(warning).toMatch(/mutable=\d[\d.]*[KMGT]?B \(\d/);
		expect(warning).toContain('allowStall=true');
		// The retention target that filled the budget, grouped by how many column
		// families carry it — the value that explained the production wedge.
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

		// getStats()/getStat() are the scrape surfaces and must agree with the
		// static accessor — an operator reading only one of them must not be told
		// the process is healthy.
		expect(last.getStats.stallActive).toBe(1);
		expect(last.getStats.bufferSize).toBe(last.stats.bufferSize);
		expect(last.getStat.stallActive).toBe(1);
		expect(last.getStat.bufferSize).toBe(last.stats.bufferSize);
		expect(last.getStat.stallActiveMs).toBe(last.getStats.stallActiveMs);

		// The `'log.warn'` event is the programmatic half of the warn line: same
		// payload, same once-per-episode cadence.
		const warned = result.stdout
			.split(/\r?\n/)
			.filter((line) => line.startsWith('WARNED '))
			.map((line) => line.slice('WARNED '.length));
		expect(warned).toHaveLength(1);
		expect(warned[0]).toBe(warning);

		// stallActiveMs is a duration, not a flag: it has to climb across samples.
		expect(stalled.at(-1)!.stats.stallActiveMs).toBeGreaterThan(stalled[0].stats.stallActiveMs);
	}, 150_000);
});
