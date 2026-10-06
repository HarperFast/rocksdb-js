import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixture = join(__dirname, 'fixtures', 'fork-error-object-failure.mts');

function runFixture(
	missingDir: string
): Promise<{ code: number | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixture, missingDir]);
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		const timeout = setTimeout(() => {
			child.kill();
			reject(new Error(`Error-object fixture timed out\n${stderr}`));
		}, 10_000);
		child.on('error', (error) => {
			clearTimeout(timeout);
			reject(error);
		});
		child.on('close', (code) => {
			clearTimeout(timeout);
			resolve({ code, stdout, stderr });
		});
	});
}

describe('native error construction failure', () => {
	it('settles the promise with the exception a failed error builder throws', async () => {
		const missingDir = join(tmpdir(), `rocksdb-js-missing-${process.pid}-${Date.now()}`);
		const { code, stdout, stderr } = await runFixture(missingDir);
		expect(stderr).toBe('');
		expect(code).toBe(0);
		expect(stdout).toContain('settled');
	}, 15_000);
});
