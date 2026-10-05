import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixturePath = join(__dirname, 'fixtures', 'fork-vt-shared-version-collision.mts');

describe('Verification table slot collisions', () => {
	it('never vouches for a key through a colliding key with the same version', () => {
		const dbPath = generateDBPath();
		try {
			const child = spawnSync(process.execPath, [fixturePath, dbPath], {
				encoding: 'utf8',
				timeout: 25000,
			});
			expect(child.error, child.stderr).toBeUndefined();
			expect(child.status, child.stderr).toBe(0);
			expect(child.stdout.trim()).toBe('ok');
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(dbPath, { recursive: true, force: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	});
});
