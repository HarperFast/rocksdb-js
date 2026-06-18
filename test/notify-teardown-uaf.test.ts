import { generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-notify-teardown-uaf.mts');

/**
 * Runs the repro fixture in a child process so a SIGABRT (the harper#1370
 * crash) surfaces as a signal/non-zero exit instead of taking down vitest.
 * The notify-vs-teardown race only fires on a fraction of attempts, so we loop
 * to give CI a useful detection rate while keeping wall time bounded.
 */
async function expectSurvives(iterations = 3): Promise<void> {
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
	it(
		'should survive worker env teardown racing an in-flight committed notify (main + worker)',
		() => expectSurvives(),
		60_000
	);
});
