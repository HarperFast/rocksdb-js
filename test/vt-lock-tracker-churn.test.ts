import { generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-vt-lock-tracker-churn.mts');

/**
 * Runs the VT LockTracker churn repro in a child process so a native abort
 * (SIGABRT/SIGSEGV from the holder/refcount double-release, or a
 * TransactionHandle::txn UAF) surfaces as a signal/non-zero exit instead of
 * taking down vitest (HarperFast/rocksdb-js#741).
 *
 * ROCKSDB_JS_TXN_CLOSE_DELAY_MS widens the close()-vs-in-flight-commit window
 * (same seam as txn-close-commit-uaf.test.ts) so the race reproduces
 * deterministically within a bounded duration.
 */
async function expectSurvives(iterations = 2): Promise<void> {
	for (let i = 0; i < iterations; i++) {
		const { code, signal } = await spawnRepro(generateDBPath());
		expect(signal, `iteration=${i}`).toBeNull();
		expect(code, `iteration=${i}`).toBe(0);
	}
}

function spawnRepro(
	dbPath: string
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		const args =
			process.versions.bun || process.versions.deno
				? [fixturePath, dbPath, '12000', '4', '1500']
				: ['node_modules/tsx/dist/cli.mjs', fixturePath, dbPath, '12000', '4', '1500'];

		const child = spawn(process.execPath, args, {
			env: { ...process.env, ROCKSDB_JS_TXN_CLOSE_DELAY_MS: '15' },
		});

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

describe('VT LockTracker holder/refcount vs. worker-env churn', () => {
	it(
		'should survive coordinatedRetry churn with the VT materialized while workers recycle (HarperFast/rocksdb-js#741)',
		() => expectSurvives(),
		120_000
	);
});
