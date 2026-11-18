import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-async-exception.mts');
const purgeFixturePath = join(__dirname, 'fixtures', 'fork-purge-claim-alloc.mts');

type Rejection = { op: string; code?: string; message?: string };

function runFixture(
	stage: number,
	options: { op?: string; env?: Record<string, string> } = {}
): Promise<Rejection[]> {
	const dir = generateDBPath();
	return new Promise<{
		code: number | null;
		signal: NodeJS.Signals | null;
		stdout: string;
		stderr: string;
	}>((resolve, reject) => {
		const args = [fixturePath, dir, String(stage)];
		if (options.op) {
			args.push(options.op);
		}
		const child = spawn(process.execPath, args, { env: { ...process.env, ...options.env } });
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		// A stranded async-work registration parks close() forever.
		const timer = setTimeout(() => child.kill('SIGKILL'), 50_000);
		child.on('close', (code, signal) => {
			clearTimeout(timer);
			resolve({ code, signal, stdout, stderr });
		});
		child.on('error', reject);
	})
		.then(({ code, signal, stdout, stderr }) => {
			if (code !== 0 || signal) {
				console.error(`Fixture stderr:\n${stderr}`);
			}
			expect({ code, signal }).toEqual({ code: 0, signal: null });
			return JSON.parse(stdout) as Rejection[];
		})
		.finally(() => rmSync(dir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 }));
}

describe('C++ exceptions in async operations', () => {
	// The containment under test is native and the fixture runs under Node; Bun and Deno load the
	// binding through their own N-API shims.
	describe.skipIf(Boolean(process.versions.deno || process.versions.bun))(
		'reject instead of terminating, and close() still returns',
		() => {
			it.each([
				[1, 'allocating the async state', { code: 'ERR_OUT_OF_MEMORY', message: 'Out of memory' }],
				[2, 'in the worker', { code: undefined, message: expect.stringMatching(/Memory limit/) }],
				[3, 'in the completion', { code: 'ERR_OUT_OF_MEMORY', message: 'Out of memory' }],
			])(
				'stage %i (%s)',
				async (stage, _description, expected) => {
					const rejections = await runFixture(stage);
					expect(rejections.length).toBeGreaterThanOrEqual(19);
					for (const { op, code, message } of rejections) {
						expect({ op, message }).toEqual({ op, message: expected.message });
						if (expected.code) {
							expect({ op, code }).toEqual({ op, code: expected.code });
						}
					}
				},
				60_000
			);

			it('skips a pin-release purge whose close claim cannot be allocated', async () => {
				const dir = generateDBPath();
				try {
					const { code, signal, stdout, stderr } = await new Promise<{
						code: number | null;
						signal: NodeJS.Signals | null;
						stdout: string;
						stderr: string;
					}>((resolve, reject) => {
						const child = spawn(process.execPath, [purgeFixturePath, dir]);
						let stdout = '';
						let stderr = '';
						child.stdout.on('data', (chunk) => {
							stdout += chunk.toString();
						});
						child.stderr.on('data', (chunk) => {
							stderr += chunk.toString();
						});
						const timer = setTimeout(() => child.kill('SIGKILL'), 50_000);
						child.on('close', (code, signal) => {
							clearTimeout(timer);
							resolve({ code, signal, stdout, stderr });
						});
						child.on('error', reject);
					});
					expect({ code, signal, stderr }).toEqual({ code: 0, signal: null, stderr: '' });
					// The entry outlives the failed retry, unclaimed, and the next close purges it.
					expect(JSON.parse(stdout)).toEqual({
						afterFailedPurge: [{ refCount: 1 }],
						value: 'value',
						afterReopenClose: 0,
					});
				} finally {
					rmSync(dir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
				}
			}, 60_000);

			it.each(['0', '2'])(
				'allocating the commit state with ROCKSDB_JS_COMMIT_THREAD=%s',
				async (mode) => {
					const rejections = await runFixture(1, {
						op: 'commit',
						env: { ROCKSDB_JS_COMMIT_THREAD: mode },
					});
					expect(rejections).toEqual([
						{ op: 'commit', code: 'ERR_OUT_OF_MEMORY', message: 'Out of memory' },
					]);
				},
				60_000
			);
		}
	);
});
