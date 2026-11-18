import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-commit-admission-shutdown.mts');

describe.each(['0', '1', '2'])('Commit admission vs. shutdown (mode %s)', (mode) => {
	it.each([
		['cold', 'before'],
		['warm', 'before'],
		['cold', 'after'],
		['warm', 'after'],
	])(
		'rejects after foreign shutdown with a %s completion cache, paused %s registration',
		async (cache, phase) => {
			const result = await new Promise<{
				code: number | null;
				signal: string | null;
				output: string;
			}>((resolve, reject) => {
				const child = spawn(process.execPath, [fixturePath, generateDBPath(), cache, phase], {
					env: { ...process.env, ROCKSDB_JS_COMMIT_THREAD: mode },
				});
				let output = '';
				child.stdout.on('data', (chunk) => (output += chunk));
				child.stderr.on('data', (chunk) => (output += chunk));
				const deadline = setTimeout(() => child.kill('SIGKILL'), 20000);
				child.once('error', (error) => {
					clearTimeout(deadline);
					reject(error);
				});
				child.once('close', (code, signal) => {
					clearTimeout(deadline);
					resolve({ code, signal, output });
				});
			});
			expect(result, result.output).toMatchObject({ code: 0, signal: null });
			expect(result.output).toContain('SUCCESS');
		},
		30000
	);
});
