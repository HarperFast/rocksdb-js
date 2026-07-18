import { generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
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
	for (let i = 0; i < iterations; i++) {
		const { code, signal } = await spawnRepro(generateDBPath(), commitThreadMode);
		expect(signal, `iteration=${i}`).toBeNull();
		expect(code, `iteration=${i}`).toBe(0);
	}
}

function spawnRepro(
	dbPath: string,
	commitThreadMode: string | undefined
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		const args =
			process.versions.bun || process.versions.deno
				? [fixturePath, dbPath]
				: ['node_modules/tsx/dist/cli.mjs', fixturePath, dbPath];

		// Widen the commit-thread completion window via the test seam so the
		// completion-vs-teardown race reproduces deterministically; natural
		// timing only surfaces it at production scale.
		const env: NodeJS.ProcessEnv = { ...process.env, ROCKSDB_JS_COMMIT_DELAY_MS: '25' };
		if (commitThreadMode === undefined) {
			delete env.ROCKSDB_JS_COMMIT_THREAD;
		} else {
			env.ROCKSDB_JS_COMMIT_THREAD = commitThreadMode;
		}
		const child = spawn(process.execPath, args, { env });

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

describe('Async commit completion vs. worker env teardown', () => {
	it.each(COMMIT_THREAD_MODES)(
		'should survive worker env teardown with commits in flight on the shared commit thread (ROCKSDB_JS_COMMIT_THREAD=$label)',
		({ mode }) => expectSurvives(mode),
		// Worker spawn/teardown dominates wall time and is slow on macOS/Windows.
		120_000
	);
});
