import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(import.meta.dirname, 'fixtures', 'fork-lingering-txn-shutdown.mts');

/**
 * Repro for HarperFast/rocksdb-js#741's precondition: worker envs that exit
 * with PENDING transactions leak their TransactionHandle into the shared
 * DBDescriptor (the JS finalizer only drops the JS-side shared_ptr; only
 * commit/abort call transactionRemove), and the leaked handle's weak napi
 * refs + env pointer outlive the env. Teardown of such an env crashes in
 * Node's second-pass napi finalizer drain (EnqueueFinalizer lambda touching
 * freed env state during Environment::RunCleanup).
 *
 * Measured (macOS, Node 24.16, Guard Malloc):
 *   - main @ 3ea9a0fb:      4/4 SIGSEGV with 10 leakers, 4/4 clean with 0
 *   - PR #745 @ 1fc79f55:   5/6 SIGSEGV — the #745 guards do not cover this
 *
 * The default macOS allocator tolerates the bad access silently, so on darwin
 * the child runs under Guard Malloc to turn it into an immediate fault. On
 * glibc the corruption surfaces natively as delayed heap aborts (the #741
 * production signature); this test has not yet been calibrated on Linux.
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
		const env: NodeJS.ProcessEnv = { ...process.env };
		if (process.platform === 'darwin') {
			// Guard Malloc: freed pages become inaccessible, so the finalizer
			// drain's use-after-free faults at the bad access instead of
			// silently reading recycled heap.
			env.DYLD_INSERT_LIBRARIES = '/usr/lib/libgmalloc.dylib';
			env.MallocScribble = '1';
		}

		const child = spawn(process.execPath, [fixturePath, dbPath, '10'], { env });

		let stderr = '';
		child.stderr?.on('data', (chunk) => {
			const text = chunk.toString();
			// Guard Malloc banner noise is not diagnostic.
			if (!text.startsWith('GuardMalloc[')) stderr += text;
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

describe('pending transactions leaked by dead worker envs', () => {
	// Reproduces on main AND on the #745 fix branch (see module doc), so this
	// stays skipped until the underlying leak/teardown bug is fixed; enabling
	// it now would just make CI red. Unskip as the acceptance test for that
	// fix: it must pass repeatedly with leakers > 0.
	it.skip(
		'should survive worker exits that leave pending transactions (HarperFast/rocksdb-js#741)',
		() => expectSurvives(),
		120_000
	);

	it('control: survives worker churn with no pending transactions leaked', async () => {
		const { code, signal } = await new Promise<{
			code: number | null;
			signal: NodeJS.Signals | null;
		}>((resolve, reject) => {
			const env: NodeJS.ProcessEnv = { ...process.env };
			if (process.platform === 'darwin') {
				env.DYLD_INSERT_LIBRARIES = '/usr/lib/libgmalloc.dylib';
				env.MallocScribble = '1';
			}
			const child = spawn(process.execPath, [fixturePath, generateDBPath(), '0'], { env });
			child.on('close', (code, signal) => resolve({ code, signal }));
			child.on('error', reject);
		});
		expect(signal).toBeNull();
		expect(code).toBe(0);
	}, 120_000);
});
