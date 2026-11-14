import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-config-atomic.mts');
const MiB = 1024 * 1024;
const lifecycleError = 'RangeError: Lifecycle wait seconds must be a positive integer';

/** config() settings are process-global, so each case runs in its own process. */
function runFixture(mode: 'beforeOpen' | 'live'): any {
	const dbPath = generateDBPath();
	try {
		const child = spawnSync(process.execPath, [fixturePath, dbPath, mode], {
			encoding: 'utf8',
			timeout: 60_000,
		});
		expect(child.status, child.stderr).toBe(0);
		const line = /^CONFIG (.*)$/m.exec(child.stdout);
		expect(line, child.stdout).not.toBeNull();
		return JSON.parse(line![1]);
	} finally {
		if (!process.env.KEEP_FILES) {
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	}
}

describe('config()', () => {
	it('should apply no field of a rejected call before any database is opened', () => {
		const result = runFixture('beforeOpen');
		expect(result.lifecycleRejected).toBe(lifecycleError);
		expect(result.vtRejected).toBe(
			'RangeError: Verification table entries must be a positive integer or 0 to disable verification'
		);
		expect(result.getterRejected).toBe('Error: getter threw');
		expect(result.settings).toEqual({
			allowStall: false,
			bufferSize: 0,
			costToCache: false,
			enabled: false,
			flushPolicy: 'oldest',
		});
		expect(result.blockCacheCapacity).toBe(32 * MiB);
		expect(result.afterOpen.enabled).toBe(false);
	}, 90_000);

	it('should apply no field of a rejected call to a live WriteBufferManager', () => {
		const result = runFixture('live');
		const created = {
			allowStall: false,
			bufferSize: 16 * MiB,
			costToCache: false,
			enabled: true,
			flushPolicy: 'oldest',
		};
		expect(result.created).toEqual(created);
		expect(result.stallRejected).toBe(lifecycleError);
		expect(result.vtRejected).toBe(
			'Error: Verification table size cannot be changed after the first database is opened'
		);
		expect(result.costToCacheRejected).toMatch(
			/^Error: writeBufferManagerCostToCache cannot be changed/
		);
		expect(result.afterRejected).toEqual(created);
		expect(result.applied).toEqual({
			allowStall: true,
			bufferSize: 32 * MiB,
			costToCache: false,
			enabled: true,
			flushPolicy: 'largest',
		});
	}, 90_000);
});
