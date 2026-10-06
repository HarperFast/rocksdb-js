import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixture = join(__dirname, 'fixtures', 'fork-error-object-failure.mts');

function runFixture(
	dbPath: string,
	missingDir: string
): Promise<{ code: number | null; stdout: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixture, dbPath, missingDir]);
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
			resolve({ code, stdout });
		});
	});
}

describe('native error construction failure', () => {
	it('settles every promise and throw with the exception a failed error builder throws', async () => {
		const suffix = `${process.pid}-${Date.now()}`;
		const dbPath = join(tmpdir(), `rocksdb-js-error-object-db-${suffix}`);
		try {
			const { code, stdout } = await runFixture(
				dbPath,
				join(tmpdir(), `rocksdb-js-error-object-missing-${suffix}`)
			);
			expect(code).toBe(0);
			expect(stdout).toContain('settled');
		} finally {
			rmSync(dbPath, { recursive: true, force: true });
		}
	}, 15_000);
});
