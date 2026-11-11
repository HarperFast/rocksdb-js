import { runChild } from './lib/child-process.ts';
import { generateDBPath } from './lib/util.ts';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-commit-teardown.mts');

// Default (single-lane) and two-lane pipeline both have their own
// completion-vs-teardown window; legacy (`0`) doesn't use the commit thread
// at all, so it's out of scope for this repro.
const COMMIT_THREAD_MODES: Array<{ label: string; mode: string | undefined }> = [
	{ label: 'default', mode: undefined },
	{ label: '2', mode: '2' },
];

const retry = process.versions.deno && process.platform === 'darwin' ? 1 : 0;

const TEST_TIMEOUT_MS = 120_000;
// Below the test timeout, so a hung child is killed and reaped and the failure
// carries its stderr rather than a bare Vitest timeout.
const CHILD_BUDGET_MS = 90_000;

/**
 * Runs the repro fixture in a child process so a native abort (SIGABRT/SIGSEGV)
 * from an async-commit completion racing worker-env teardown surfaces as a
 * signal / non-zero exit instead of taking down vitest. Loops a few iterations
 * to give CI a useful detection rate while keeping wall time bounded.
 */
// One iteration where worker spawns are slow (Bun; macOS/Windows CI), two on
// Linux Node — see the fixture's ROUNDS note.
async function expectSurvives(
	commitThreadMode: string | undefined,
	iterations = process.versions.bun || process.platform === 'darwin' || process.platform === 'win32'
		? 1
		: 2
): Promise<void> {
	// Widen the commit-thread completion window via the test seam so the
	// completion-vs-teardown race reproduces deterministically; natural
	// timing only surfaces it at production scale.
	const env: NodeJS.ProcessEnv = { ...process.env, ROCKSDB_JS_COMMIT_DELAY_MS: '25' };
	if (commitThreadMode === undefined) {
		delete env.ROCKSDB_JS_COMMIT_THREAD;
	} else {
		env.ROCKSDB_JS_COMMIT_THREAD = commitThreadMode;
	}
	const deadlineMs = Math.floor(CHILD_BUDGET_MS / iterations);

	for (let i = 0; i < iterations; i++) {
		const { code, signal, stderr, timedOut } = await runChild(
			process.execPath,
			[fixturePath, generateDBPath()],
			{ deadlineMs, env }
		);
		const context = `ROCKSDB_JS_COMMIT_THREAD=${commitThreadMode ?? 'default'} iteration=${i}\nRepro child stderr:\n${stderr}`;
		expect(timedOut, `repro child did not exit within ${deadlineMs}ms; ${context}`).toBe(false);
		expect(signal, context).toBeNull();
		expect(code, context).toBe(0);
	}
}

describe('Async commit completion vs. worker env teardown', () => {
	it.each(COMMIT_THREAD_MODES)(
		'should survive worker env teardown with commits in flight on the shared commit thread (ROCKSDB_JS_COMMIT_THREAD=$label)',
		{
			// The single retry is limited to the pre-existing Deno/macOS crash in #746.
			retry,
			// Worker spawn/teardown dominates wall time and is slow on macOS/Windows.
			timeout: TEST_TIMEOUT_MS,
		},
		({ mode }) => expectSurvives(mode)
	);
});
