import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-occ-lock-buckets.mts');

describe('process-wide optimistic lock buckets', () => {
	for (const mode of ['default', 'configured', 'transactions']) {
		it(`shares the pool (${mode})`, () => {
			const dbPath = generateDBPath();
			try {
				const child = spawnSync(process.execPath, [fixturePath, dbPath, mode], {
					encoding: 'utf8',
					timeout: 25000,
				});
				expect(child.error, child.stderr).toBeUndefined();
				expect(child.signal, child.stderr).toBeNull();
				expect(child.status, child.stderr).toBe(0);
				const result = JSON.parse(child.stdout.trim().split(/\r?\n/).at(-1)!);
				if (result.anonymousMiB !== null) {
					expect(result.anonymousMiB).toBeLessThan(mode === 'default' ? 160 : 32);
				}
			} finally {
				if (!process.env.KEEP_FILES) {
					rmSync(dbPath, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
				}
			}
		});
	}
});
