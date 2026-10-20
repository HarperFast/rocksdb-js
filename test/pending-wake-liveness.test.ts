import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-pending-wake.mts');

/**
 * Runs one `fork-pending-wake.mts` scenario. A regression in this area is
 * either a crash or a process that never exits, so the child is killed after a
 * deadline rather than left to the runner's timeout.
 */
function spawnFixture(
	mode: string,
	commitThread?: string
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string }> {
	const env: NodeJS.ProcessEnv = { ...process.env };
	if (commitThread === undefined) {
		delete env.ROCKSDB_JS_COMMIT_THREAD;
	} else {
		env.ROCKSDB_JS_COMMIT_THREAD = commitThread;
	}
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, mode, generateDBPath()], { env });
		const deadline = setTimeout(() => child.kill('SIGKILL'), 30_000);
		let stdout = '';
		let stderr = '';
		child.stdout?.on('data', (chunk) => (stdout += chunk.toString()));
		child.stderr?.on('data', (chunk) => (stderr += chunk.toString()));
		child.on('close', (code, signal) => {
			clearTimeout(deadline);
			if (code !== 0 || signal) {
				console.error(`fork-pending-wake ${mode} stderr:\n${stderr}`);
			}
			resolve({ code, signal, stdout });
		});
		child.on('error', reject);
	});
}

async function expectSuccess(mode: string, commitThread?: string): Promise<void> {
	const { code, signal, stdout } = await spawnFixture(mode, commitThread);
	expect(signal).toBeNull();
	expect(code).toBe(0);
	expect(stdout).toContain('SUCCESS');
}

// Node-only: the guarantee under test is Node's threadsafe-function ref
// semantics and env-teardown ordering, which Deno's and Bun's N-API shims do not
// reproduce (see lock-teardown-abort.test.ts).
describe.skipIf(Boolean(process.versions.deno || process.versions.bun))(
	'Pending cross-thread wakes keep the waiting thread alive (#797)',
	{ timeout: 60_000 },
	() => {
		it('withLock() waiter', () => expectSuccess('withLock'));

		it('tryLock() onUnlocked callback', () => expectSuccess('tryLock'));

		it.each([
			{ label: 'default', commitThread: undefined },
			{ label: '0', commitThread: '0' },
			{ label: '2', commitThread: '2' },
		])(
			'coordinated-retry commit parked on another thread resumes and commits (ROCKSDB_JS_COMMIT_THREAD=$label)',
			({ commitThread }) => expectSuccess('park', commitThread)
		);

		it('uncontended same-thread withLock() runs and resolves', () => expectSuccess('same-thread'));

		it('a worker exiting with a queued lock call neither crashes the process nor strands the lock', () =>
			expectSuccess('worker-exit'));
	}
);
