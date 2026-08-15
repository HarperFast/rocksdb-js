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
 * Measured with 10 leakers:
 *   - Linux/glibc (node:24 arm64, native — the production platform):
 *       main @ 3ea9a0fb: 0/10 pass, aborting with the #741 production
 *       signatures ("corrupted size vs. prev_size", "corrupted double-linked
 *       list", "free(): invalid pointer", "malloc_consolidate(): invalid
 *       chunk size"); with CloseTransactionsByEnv + napi-free close: 10/10
 *       pass (incl. MALLOC_PERTURB_). PR #745's guards alone do not cover
 *       this (5/6 crash on macOS/gmalloc at its head).
 *   - macOS (Node 24.16): silent natively; under Guard Malloc main is 4/4
 *     SIGSEGV, and a residual gmalloc-only fault in Node's second-pass napi
 *     finalizer drain persists even with the fix — hence skipIf(darwin).
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
	// Linux/glibc + Node (the production stack #741 crashed on): without
	// the env-cleanup-hook reap (CloseTransactionsByEnv) this aborts every
	// run with the production
	// signatures ("corrupted size vs. prev_size", "corrupted double-linked
	// list", "free(): invalid pointer" — main 0/10); with it, 10/10 clean.
	//
	// macOS: a Guard-Malloc-only fault in Node's second-pass napi finalizer
	// drain persists even with the fix (never reproduces without gmalloc, and
	// never on glibc) — suspected macOS-specific Node teardown artifact,
	// tracked separately.
	//
	// Deno/Bun: gated off like notify-teardown-uaf — their worker/napi-env
	// teardown lifecycles differ from Node's (Deno's child still SIGABRTs for
	// reasons that don't apply to Harper's production runtime, and Bun's child
	// fails to start). Node is where #741 reproduces and is fixed.
	it.skipIf(
		process.platform === 'darwin' || Boolean(process.versions.deno || process.versions.bun)
	)(
		'should survive worker exits that leave pending transactions (HarperFast/rocksdb-js#741)',
		() => expectSurvives(),
		120_000
	);

	// The drain-timeout path from the #780 review. Each worker dies with a
	// commit stalled immediately before rocksdb::Transaction::Commit()
	// (ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS), so the env-cleanup reap's bounded
	// 5s drain expires while `txn` is about to be dereferenced. Before the
	// close() hardening this segfaults every run — the commit thread wakes and
	// commits through a destroyed transaction (3/3 exit 139 on macOS, no
	// allocator instrumentation needed); with it, close() leaves the
	// transaction to its in-flight commit and the process survives.
	//
	// Runs everywhere: unlike the pending-transaction repro above, this failure
	// is a plain null/dangling dereference rather than silent heap corruption,
	// so it is not platform-gated.
	it('should survive worker exits during a commit that outlasts the close drain', async () => {
		const { code, signal } = await new Promise<{
			code: number | null;
			signal: NodeJS.Signals | null;
		}>((resolve, reject) => {
			const child = spawn(process.execPath, [fixturePath, generateDBPath(), '2', 'slowcommit'], {
				env: { ...process.env, ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '6000' },
			});
			child.on('close', (code, signal) => resolve({ code, signal }));
			child.on('error', reject);
		});
		expect(signal).toBeNull();
		expect(code).toBe(0);
	}, 120_000);

	// The differentiating condition, isolated: identical topology and
	// NativeTransaction lifecycle as the pending repro above — ten sequential
	// workers each creating a transaction and exiting — except these COMMIT, so
	// transactionRemove() takes each one out of the registry before its env
	// dies. Nothing lingers, so this stays green on main as well as here.
	it('control: committed transactions survive the same worker churn', async () => {
		const { code, signal } = await new Promise<{
			code: number | null;
			signal: NodeJS.Signals | null;
		}>((resolve, reject) => {
			const env: NodeJS.ProcessEnv = { ...process.env };
			if (process.platform === 'darwin') {
				env.DYLD_INSERT_LIBRARIES = '/usr/lib/libgmalloc.dylib';
				env.MallocScribble = '1';
			}
			const child = spawn(process.execPath, [fixturePath, generateDBPath(), '10', 'committer'], {
				env,
			});
			child.on('close', (code, signal) => resolve({ code, signal }));
			child.on('error', reject);
		});
		expect(signal).toBeNull();
		expect(code).toBe(0);
	}, 120_000);
});
