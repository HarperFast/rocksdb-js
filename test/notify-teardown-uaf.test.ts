import { generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-notify-teardown-uaf.mts');

/**
 * Runs the repro fixture in a child process so a SIGABRT (the harper#1370
 * crash) surfaces as a signal/non-zero exit instead of taking down vitest.
 * The notify-vs-teardown race only fires on a fraction of attempts, so we loop
 * to give CI a useful detection rate while keeping wall time bounded. Most of
 * the per-iteration cost is worker spawn/teardown (60 rounds inside the
 * fixture), which is expensive on macOS/Windows — that, not the race-window
 * count, is what drives wall time, so the iteration count is kept low and the
 * test timeout carries headroom (see the it() timeout below).
 */
// Bun's worker lifecycle is far slower (see ROUNDS in the fixture); a single
// iteration there keeps wall time under the timeout while Node/Deno run two.
async function expectSurvives(iterations = process.versions.bun ? 1 : 2): Promise<void> {
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
				? [fixturePath, dbPath]
				: ['node_modules/tsx/dist/cli.mjs', fixturePath, dbPath];

		// Widen the notify acquire->call window via the test seam so the
		// worker-teardown-vs-notify race (harper#1370) reproduces deterministically;
		// natural timing only surfaces it at production scale.
		const child = spawn(process.execPath, args, {
			env: { ...process.env, ROCKSDB_JS_NOTIFY_DELAY_MS: '25' },
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

describe('Per-database events notify() vs. worker teardown', () => {
	// Node-only. The fix this guards (holding the emitter mutex across the tsfn
	// call) is safe because Node runs an exiting worker env's cleanup hook —
	// which blocks on that same mutex via removeListenersByEnv — *before* freeing
	// the env's tsfns. Deno and Bun have independent N-API implementations whose
	// worker.terminate() teardown does not provide that ordering guarantee, so the
	// assertion exercises their shim's teardown semantics, not our code: Deno
	// (macOS) intermittently SIGABRTs freeing the env out from under the in-lock
	// call, and Bun (Windows) can stall. Node is Harper's production runtime and
	// is where harper#1370 reproduces and is fixed.
	it.skipIf(Boolean(process.versions.deno || process.versions.bun))(
		'should survive worker env teardown racing an in-flight committed notify (main + worker)',
		() => expectSurvives(),
		// Worker spawn/teardown dominates wall time and is slow on macOS/Windows:
		// even passing runs there land ~55s for 3 iterations. With 2 iterations
		// the spec-compliant runtimes finish well under this, while the headroom
		// keeps slower runtimes (Node 22, Bun) from flaking on the timeout itself
		// rather than on an actual crash.
		120_000
	);
});
