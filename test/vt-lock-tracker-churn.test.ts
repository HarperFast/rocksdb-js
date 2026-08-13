import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(import.meta.dirname, 'fixtures', 'fork-vt-lock-tracker-churn.mts');

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
		// Recycle interval (4th arg) intentionally matches the proven-safe
		// repro-crossthread.mjs cadence rather than something more aggressive --
		// see the fixture's module doc for why.
		const args = [fixturePath, dbPath, '12000', '4', '4000'];

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
	// STILL SKIPPED. The heap-use-after-free that dominated this fixture is
	// fixed (napi_delete_reference called from the env's own cleanup hook after
	// Node had already freed the env's N-API state -- see napi/env_teardown.h),
	// and the library's own repro is now clean: repro-crossthread.mjs went 9/12
	// crashing -> 0/12 over 12 runs of ~1M transactions each, and stays clean
	// 8/8 at an even harsher 800ms recycle.
	//
	// But this fixture still fails ~25% of the time (1/4 reps at the cadence
	// below, 1/6 at 1500ms) with the same `corrupted size vs. prev_size`. That
	// residual crash only reproduces through this tsx-transpiled-worker path,
	// not through the plain-.mjs repro, and ThreadSanitizer did not catch it
	// (0 races over 3 aggressive runs -- TSan's ~15x slowdown likely closes the
	// window). It is NOT root-caused, so enabling this would make CI flaky for
	// a reason this PR does not address. See AGENTS.md item 12.
	it.skip(
		'should survive coordinatedRetry churn with the VT materialized while workers recycle (HarperFast/rocksdb-js#741)',
		() => expectSurvives(),
		120_000
	);
});
