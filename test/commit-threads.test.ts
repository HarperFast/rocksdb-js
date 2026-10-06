import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-commit-threads.mts');

function runFixture(args: string[], env: Record<string, string> = {}): Record<string, any> {
	const dbPath = generateDBPath();
	try {
		const child = spawnSync(process.execPath, [fixturePath, dbPath, ...args], {
			encoding: 'utf8',
			env: { ...process.env, ...env },
			timeout: 60000,
		});
		expect(child.error, child.stderr).toBeUndefined();
		expect(child.signal, child.stderr).toBeNull();
		expect(child.status, child.stderr).toBe(0);
		return JSON.parse(child.stdout.trim().split(/\r?\n/).at(-1)!);
	} finally {
		if (!process.env.KEEP_FILES) {
			for (const path of [dbPath, `${dbPath}-other`]) {
				rmSync(path, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	}
}

describe('commitThreads', () => {
	it('validates the setting and starts threads only for overlapping commits', () => {
		runFixture(['validate']);
	});

	describe.each(['1', '2'])('ROCKSDB_JS_COMMIT_THREAD=%s', (commitThread) => {
		it('runs one commit at a time with one thread', () => {
			const result = runFixture(['concurrent', '1'], {
				ROCKSDB_JS_COMMIT_THREAD: commitThread,
				ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '100',
			});
			expect(result.threads).toBe(1);
			expect(result.wallMs).toBeGreaterThanOrEqual(790);
		});

		it('runs up to commitThreads commits at once', () => {
			const result = runFixture(['concurrent', '4'], {
				ROCKSDB_JS_COMMIT_THREAD: commitThread,
				ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '100',
			});
			expect(result.threads).toBe(4);
			expect(result.wallMs).toBeLessThan(600);
		});

		// Deno runs node:fs outside the pool that N-API async work uses, so this cannot fail there.
		it.skipIf(Boolean(process.versions.deno))(
			'leaves the libuv threadpool free while commits are stalled',
			() => {
				const result = runFixture(['starve', '4'], {
					ROCKSDB_JS_COMMIT_THREAD: commitThread,
					ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '300',
					UV_THREADPOOL_SIZE: '2',
				});
				expect(result.statMs).toBeLessThan(150);
			}
		);

		it('resolves commits in dispatch order with one thread', () => {
			const result = runFixture(['order', '1'], { ROCKSDB_JS_COMMIT_THREAD: commitThread });
			expect(result.resolved).toEqual(['big', 'small']);
		});

		it('lets a later commit finish first and keeps log entries behind their data', () => {
			const result = runFixture(['order', '4'], { ROCKSDB_JS_COMMIT_THREAD: commitThread });
			expect(result.resolved).toEqual(['small', 'big']);
		});
	});
});
