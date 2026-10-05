import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-occ-lock-buckets.mts');

function runFixture(mode: string): Record<string, number | null> {
	const dbPath = generateDBPath();
	try {
		const child = spawnSync(process.execPath, [fixturePath, dbPath, mode], {
			encoding: 'utf8',
			timeout: 25000,
		});
		expect(child.error, child.stderr).toBeUndefined();
		expect(child.signal, child.stderr).toBeNull();
		expect(child.status, child.stderr).toBe(0);
		return JSON.parse(child.stdout.trim().split(/\r?\n/).at(-1)!);
	} finally {
		if (!process.env.KEEP_FILES) {
			rmSync(dbPath, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
		}
	}
}

// RocksDB's own default is 2^20 buckets per database, about 40 MiB each on Linux x64. The memory
// assertions only run on Linux, where anonymous memory is readable from /proc.
describe('per-database optimistic lock buckets', () => {
	it('keeps ten empty databases small at the default count', () => {
		const result = runFixture('default');
		if (result.defaultGrowthMiB !== null) {
			expect(result.defaultGrowthMiB).toBeLessThan(100);
		}
	});

	it('applies the configured count to subsequent opens only', () => {
		const result = runFixture('configured');
		if (result.largeGrowthMiB !== null) {
			expect(result.largeGrowthMiB).toBeGreaterThan(90);
			expect(result.smallGrowthMiB).toBeLessThan(30);
		}
	});

	it('detects conflicts and commits large transactions with 16 buckets', () => {
		runFixture('transactions');
	});
});
