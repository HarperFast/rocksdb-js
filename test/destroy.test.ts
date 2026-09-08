import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('Destroy', () => {
	// destroy() ends in remove_all(), so it must never turn "no path" into a
	// path. A handle that was never opened has none, and resolving an empty
	// string hands back the process working directory on libc++ (the standard's
	// current_path() / p) — deleting the directory the process runs in. Driven
	// from a child with a throwaway CWD so a regression here cannot reach
	// anything real.
	it('should refuse to destroy a database that was never opened', async () => {
		const dbPath = generateDBPath();
		const cwd = `${dbPath}-cwd`;
		mkdirSync(cwd, { recursive: true });
		writeFileSync(join(cwd, 'sentinel.txt'), 'keep me');
		try {
			const { code, output } = await new Promise<{ code: number | null; output: string }>(
				(resolve, reject) => {
					const child = spawn(
						process.execPath,
						[join(__dirname, 'fixtures', 'fork-destroy-unopened.mts'), dbPath],
						{ cwd }
					);
					let output = '';
					child.stdout.on('data', (chunk) => (output += chunk));
					child.stderr.on('data', (chunk) => (output += chunk));
					child.on('close', (code) => resolve({ code, output }));
					child.on('error', reject);
				}
			);
			expect(code, output).toBe(0);
			expect(existsSync(join(cwd, 'sentinel.txt'))).toBe(true);
		} finally {
			rmSync(cwd, { force: true, recursive: true });
		}
	});

	it('should destroy a closed database', () =>
		dbRunner(async ({ db, dbPath }) => {
			expect(db.isOpen()).toBe(true);
			db.close();
			expect(existsSync(dbPath)).toBe(true);
			expect(db.isOpen()).toBe(false);
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
			expect(db.isOpen()).toBe(false);
		}));

	it.skipIf(process.platform === 'win32')(
		'should destroy the database opened before its symlink was repointed',
		() => {
			const dbPath = generateDBPath();
			const replacementPath = `${dbPath}-replacement`;
			const linkPath = `${dbPath}-link`;
			mkdirSync(dbPath, { recursive: true });
			mkdirSync(replacementPath, { recursive: true });
			writeFileSync(join(replacementPath, 'sentinel.txt'), 'keep me');
			symlinkSync(dbPath, linkPath, 'dir');

			const db = new RocksDatabase(linkPath);
			try {
				db.open();
				db.close();
				rmSync(linkPath);
				symlinkSync(replacementPath, linkPath, 'dir');

				db.destroy();
				expect(existsSync(dbPath)).toBe(false);
				expect(existsSync(join(replacementPath, 'sentinel.txt'))).toBe(true);
				expect(existsSync(linkPath)).toBe(true);
			} finally {
				db.close();
				rmSync(linkPath, { force: true });
				rmSync(dbPath, { force: true, recursive: true });
				rmSync(replacementPath, { force: true, recursive: true });
			}
		}
	);

	it('should refuse destroy after a read-only handle closes', () =>
		dbRunner({ dbOptions: [{}, { readOnly: true }] }, async ({ db, dbPath }, { db: readOnly }) => {
			readOnly.close();
			expect(() => readOnly.destroy()).toThrow('Unsupported operation in read-only mode');
			expect(existsSync(dbPath)).toBe(true);
			expect(db.isOpen()).toBe(true);
		}));

	it('should destroy an open database', () =>
		dbRunner(({ db, dbPath }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
			expect(db.isOpen()).toBe(false);
		}));

	it('should destroy all related instances', () =>
		dbRunner(
			{ dbOptions: [{}, { name: 'test' }] },
			async ({ db: db1, dbPath: dbPath1 }, { db: db2, dbPath: dbPath2 }) => {
				expect(existsSync(dbPath1)).toBe(true);
				expect(existsSync(dbPath2)).toBe(true);
				expect(db1.isOpen()).toBe(true);
				expect(db2.isOpen()).toBe(true);

				db1.destroy();

				expect(existsSync(dbPath1)).toBe(false);
				expect(existsSync(dbPath2)).toBe(false);
				expect(db1.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
			}
		));
});
