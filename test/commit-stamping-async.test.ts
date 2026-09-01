import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * Commit-stamping semantics through the ASYNC commit path, under every commit
 * execution mode (docs/design/local-mutation-stamping.md §8): the mode env var
 * is read once per process, so each mode runs in a spawned child
 * (test/fixtures/fork-stamp-modes.mts) asserting keep, re-stamp, mixed-CF, and
 * #668 pinned-retry behavior — the production shape (replicated apply → async
 * commit → rebuild on the commit lane).
 *
 * Node-only (spawned-child fixture).
 */

const fixturePath = join(import.meta.dirname, 'fixtures', 'fork-stamp-modes.mts');
const isNode = !process.versions.bun && !process.versions.deno;

const paths: string[] = [];

function dbPath(): string {
	const path = generateDBPath();
	paths.push(path);
	return path;
}

afterEach(() => {
	for (const path of paths.splice(0)) {
		rmSync(path, { recursive: true, force: true });
		rmSync(`${path}-logs`, { recursive: true, force: true });
	}
});

function runMode(path: string, mode?: string): Promise<{ code: number | null; stderr: string }> {
	return new Promise((resolve, reject) => {
		const env: NodeJS.ProcessEnv = { ...process.env };
		delete env.ROCKSDB_JS_COMMIT_THREAD;
		delete env.ROCKSDB_JS_CRASH_POINT;
		if (mode !== undefined) {
			env.ROCKSDB_JS_COMMIT_THREAD = mode;
		}
		const child = spawn(process.execPath, [fixturePath, path], { env });
		let stderr = '';
		child.stderr.on('data', (chunk) => (stderr += chunk.toString()));
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stderr }));
	});
}

describe.skipIf(!isNode)('commit stamping async lanes', () => {
	it('single-lane commit worker (default)', async () => {
		const run = await runMode(dbPath());
		expect(run.code, run.stderr).toBe(0);
	});

	it('legacy libuv threadpool (ROCKSDB_JS_COMMIT_THREAD=0)', async () => {
		const run = await runMode(dbPath(), '0');
		expect(run.code, run.stderr).toBe(0);
	});

	it('two-lane pipeline (ROCKSDB_JS_COMMIT_THREAD=2)', async () => {
		const run = await runMode(dbPath(), '2');
		expect(run.code, run.stderr).toBe(0);
	});
});
