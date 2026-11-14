import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const nodeExecutable =
	process.env.NODE_BINARY ??
	(process.versions.bun || process.versions.deno
		? (process.env.npm_node_execpath ?? 'node')
		: process.execPath);

// Windows retains a frozen segment's mapping in the log file, so the buffer never owns the last
// reference there.
describe.skipIf(process.platform === 'win32')('Transaction log mapping buffers', () => {
	it('releases the mapping when a terminated worker cannot create its buffer', () => {
		const dbPath = generateDBPath();
		try {
			const result = spawnSync(
				nodeExecutable,
				[
					'--expose-gc',
					join(import.meta.dirname, 'fixtures', 'fork-memory-map-buffer-terminate.mts'),
					dbPath,
				],
				{
					encoding: 'utf8',
					env: { ...process.env, ROCKSDB_JS_MEMORY_MAP_BUFFER_DELAY_MS: '3000' },
					timeout: 60_000,
				}
			);
			expect(result.status, result.stderr).toBe(0);
			expect(JSON.parse(result.stdout.trim().split('\n').pop()!)).toEqual({ released: true });
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	});
});
