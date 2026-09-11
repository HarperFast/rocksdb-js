import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-lock-teardown-abort.mts');

/**
 * Runs the repro in a child process so the SIGABRT (rocksdb-js#848) surfaces as
 * a signal / non-zero exit instead of taking down vitest. The failure is
 * deterministic on an affected Node, so one child run with a few rounds is
 * enough; the fixture also proves a live waiter is still woken afterwards.
 */
function spawnRepro(
	dbPath: string
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, dbPath]);
		let stderr = '';
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('close', (code, signal) => {
			if (code !== 0 || signal) {
				console.error(`Repro child stderr:\n${stderr}`);
			}
			resolve({ code, signal });
		});
		child.on('error', reject);
	});
}

describe('tryLock callback vs. worker env teardown', () => {
	// Node-only for the same reason as notify-teardown-uaf: the guarantee under
	// test is Node's env-cleanup ordering (cleanup hooks run before the env's
	// tsfns are freed), which Deno's and Bun's N-API shims do not provide.
	it.skipIf(Boolean(process.versions.deno || process.versions.bun))(
		'should survive unlock() after a waiting worker was terminated, and still wake a live waiter',
		async () => {
			const { code, signal } = await spawnRepro(generateDBPath());
			expect(signal).toBeNull();
			expect(code).toBe(0);
		},
		60_000
	);
});
