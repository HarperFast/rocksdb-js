import { registryStatus, shutdown } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('Shutdown', () => {
	it('should shutdown rocksdb-js', () =>
		dbRunner({ dbOptions: [{}, { name: 'test' }] }, async ({ db }, { db: db2 }) => {
			expect(db.isOpen()).toBe(true);
			expect(db2.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(status[0].columnFamilies.length).toBe(2);
			shutdown();
			expect(db.isOpen()).toBe(false);
			expect(db2.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));

	it('should handle multiple shutdowns', () =>
		dbRunner(async ({ db }) => {
			expect(db.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(status[0].columnFamilies.length).toBe(1);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));

	it('should open 10 databases, shutdown, and open them again', async () =>
		dbRunner(
			{
				dbOptions: [
					/*  1 */ {},
					/*  2 */ { path: generateDBPath() },
					/*  3 */ { path: generateDBPath() },
					/*  4 */ { path: generateDBPath() },
					/*  5 */ { path: generateDBPath() },
					/*  6 */ { path: generateDBPath() },
					/*  7 */ { path: generateDBPath() },
					/*  8 */ { path: generateDBPath() },
					/*  9 */ { path: generateDBPath() },
					/* 10 */ { path: generateDBPath() },
				],
			},
			async (
				{ db },
				{ db: db2 },
				{ db: db3 },
				{ db: db4 },
				{ db: db5 },
				{ db: db6 },
				{ db: db7 },
				{ db: db8 },
				{ db: db9 },
				{ db: db10 }
			) => {
				for (let i = 0; i < 10; i++) {
					db.putSync(i, i);
					db2.putSync(i, i);
					db3.putSync(i, i);
					db4.putSync(i, i);
					db5.putSync(i, i);
					db6.putSync(i, i);
					db7.putSync(i, i);
					db8.putSync(i, i);
					db9.putSync(i, i);
					db10.putSync(i, i);
				}

				shutdown();

				expect(db.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
				expect(db3.isOpen()).toBe(false);
				expect(db4.isOpen()).toBe(false);
				expect(db5.isOpen()).toBe(false);
				expect(db6.isOpen()).toBe(false);
				expect(db7.isOpen()).toBe(false);
				expect(db8.isOpen()).toBe(false);
				expect(db9.isOpen()).toBe(false);
				expect(db10.isOpen()).toBe(false);

				db.open();
				db2.open();
				db3.open();
				db4.open();
				db5.open();
				db6.open();
				db7.open();
				db8.open();
				db9.open();
				db10.open();

				expect(db.isOpen()).toBe(true);
				expect(db2.isOpen()).toBe(true);
				expect(db3.isOpen()).toBe(true);
				expect(db4.isOpen()).toBe(true);
				expect(db5.isOpen()).toBe(true);
				expect(db6.isOpen()).toBe(true);
				expect(db7.isOpen()).toBe(true);
				expect(db8.isOpen()).toBe(true);
				expect(db9.isOpen()).toBe(true);
				expect(db10.isOpen()).toBe(true);

				for (let i = 0; i < 10; i++) {
					expect(db.getSync(i)).toBe(i);
					expect(db2.getSync(i)).toBe(i);
					expect(db3.getSync(i)).toBe(i);
					expect(db4.getSync(i)).toBe(i);
					expect(db5.getSync(i)).toBe(i);
					expect(db6.getSync(i)).toBe(i);
					expect(db7.getSync(i)).toBe(i);
					expect(db8.getSync(i)).toBe(i);
					expect(db9.getSync(i)).toBe(i);
					expect(db10.getSync(i)).toBe(i);
				}
			}
		));

	it('should spawn process that opens 20 databases, shutdown on exit, forks, an open them again', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });

		await new Promise<void>((resolve, reject) => {
			const args =
				process.versions.bun || process.versions.deno
					? [join(__dirname, 'fixtures', 'fork-shutdown.mts'), dbPath]
					: [
							'node_modules/tsx/dist/cli.mjs',
							join(__dirname, 'fixtures', 'fork-shutdown.mts'),
							dbPath,
						];

			const child = spawn(process.execPath, args, {
				env: { ...process.env, DO_FORK: '1' },
				// stdio: 'inherit',
			});
			child.on('close', (code) => {
				try {
					expect(code).toBe(0);
					resolve();
				} catch (error) {
					reject(error);
				}
			});
			child.on('error', reject);
		});
	});
});
