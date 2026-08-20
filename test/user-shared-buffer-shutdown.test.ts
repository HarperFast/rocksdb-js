import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * Regression repro for the user-shared-buffer finalize use-after-free seen on
 * Harper shutdown (ctrl-C) on macOS:
 *
 *   userSharedBufferFinalize -> napi_get_reference_value  (SIGSEGV)
 *   ... Environment::RunCleanup -> FreeEnvironment (teardown)
 *
 * `userSharedBufferFinalize` reads the buffer's borrowed `callbackRef`, but that
 * napi_ref is owned by the listener's threadsafe function and has already been
 * deleted once the DB (and its listeners) were torn down at shutdown. See
 * `test/fixtures/fork-user-shared-buffer-shutdown.mts` for the full scenario.
 *
 * Runs in a child process because a regression SIGSEGVs rather than throwing a
 * catchable error. On a plain build the freed napi_ref slot usually reads back
 * benign, so the child is run under Apple's Guard Malloc to make the UAF fault
 * deterministically. This is macOS-only: Guard Malloc is macOS-specific, ASan
 * does not work locally on recent macOS (see AGENTS.md), and a plain run on any
 * other platform is a false green (and can flake if the freed ref slot gets
 * reused), so it is skipped there rather than run without instrumentation.
 */
describe('User shared buffer shutdown use-after-free repro (child process)', () => {
	const fixturePath = join(__dirname, 'fixtures', 'fork-user-shared-buffer-shutdown.mts');
	const GUARD_MALLOC = '/usr/lib/libgmalloc.dylib';

	// macOS + Guard Malloc + Node only (Bun/Deno don't run this fixture under
	// Node's native type stripping the same way).
	const runnable =
		process.platform === 'darwin' &&
		existsSync(GUARD_MALLOC) &&
		!process.versions.bun &&
		!process.versions.deno;

	it.runIf(runnable)(
		'should survive shutdown while user shared buffers with callbacks are still in use',
		async () => {
			// Loop: teardown-order UAFs only fault on a fraction of runs even under
			// Guard Malloc, so a single shot can mask a regression.
			for (let i = 0; i < 5; i++) {
				const { code, signal } = await spawnShutdownRepro(fixturePath);
				expect(signal, `iteration=${i}`).toBeNull();
				expect(code, `iteration=${i}`).toBe(0);
			}
		},
		30_000
	);
});

function spawnShutdownRepro(
	fixturePath: string
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		const dbPath = generateDBPath();
		// Fault immediately on the use-after-free instead of reading benign freed
		// memory. MallocScribble poisons freed blocks so a stale read is more
		// likely to be caught even outside the guard page.
		const env = {
			...process.env,
			DYLD_INSERT_LIBRARIES: '/usr/lib/libgmalloc.dylib',
			MallocScribble: '1',
		};

		const child = spawn(process.execPath, [fixturePath, dbPath], { env });

		let stderr = '';
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('close', (code, signal) => {
			if (code !== 0 || signal) {
				console.error(`Shutdown repro stderr:\n${stderr}`);
			}
			resolve({ code, signal });
		});
		child.on('error', reject);
	});
}
