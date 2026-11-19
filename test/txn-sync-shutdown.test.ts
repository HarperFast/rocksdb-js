import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

function runFixture(fixture: string, args: string[]) {
	return new Promise<{ code: number | null; signal: string | null; output: string }>(
		(resolve, reject) => {
			const child = spawn(process.execPath, [join(__dirname, 'fixtures', fixture), ...args]);
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
		}
	);
}

describe('Synchronous transaction calls vs. a foreign shutdown', () => {
	describe.each(['txn', 'db'])('through the %s context', (context) => {
		it.each(['getSync', 'putSync', 'removeSync', 'getRange'])(
			'%s holds shutdown off until it returns',
			async (operation) => {
				const result = await runFixture('fork-txn-sync-shutdown.mts', [
					generateDBPath(),
					operation,
					context,
				]);
				expect(result, result.output).toMatchObject({ code: 0, signal: null });
				expect(result.output).toContain('SUCCESS');
			},
			30000
		);
	});

	it.each(['closing', 'detached'])(
		'refuses every entry point once its database is %s',
		async (mode) => {
			const result = await runFixture('fork-txn-sync-refusal.mts', [generateDBPath(), mode]);
			expect(result, result.output).toMatchObject({ code: 0, signal: null });
			expect(result.output).toContain('SUCCESS');
		},
		30000
	);
});
