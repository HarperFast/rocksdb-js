import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const destroyOpenFixture = join(__dirname, 'fixtures', 'fork-destroy-open.mts');
const destroyFailureFixture = join(__dirname, 'fixtures', 'fork-destroy-failure.mts');

function runDestroyFixture(
	fixture: string,
	dbPath: string,
	env?: NodeJS.ProcessEnv
): Promise<void> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixture, dbPath], {
			env: { ...process.env, ...env },
		});
		let stderr = '';
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		const timeout = setTimeout(() => {
			child.kill();
			reject(new Error(`Destroy fixture timed out\n${stderr}`));
		}, 10_000);
		child.on('error', reject);
		child.on('close', (code, signal) => {
			clearTimeout(timeout);
			if (code === 0 && signal === null) {
				resolve();
			} else {
				reject(new Error(`Destroy fixture failed (code=${code}, signal=${signal})\n${stderr}`));
			}
		});
	});
}

describe('Destroy', () => {
	it('validates the lifecycle wait configuration', () => {
		expect(() => RocksDatabase.config({ lifecycleWaitSeconds: 0 })).toThrow(
			'Lifecycle wait seconds must be a positive integer'
		);
		expect(() => RocksDatabase.config({ lifecycleWaitSeconds: 30 })).not.toThrow();
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
			{ dbOptions: [{}, { name: 'test' }, { readOnly: true }] },
			async (
				{ db: db1, dbPath: dbPath1 },
				{ db: db2, dbPath: dbPath2 },
				{ db: readOnly, dbPath: readOnlyPath }
			) => {
				expect(existsSync(dbPath1)).toBe(true);
				expect(existsSync(dbPath2)).toBe(true);
				expect(existsSync(readOnlyPath)).toBe(true);
				expect(db1.isOpen()).toBe(true);
				expect(db2.isOpen()).toBe(true);
				expect(readOnly.isOpen()).toBe(true);

				db1.destroy();

				expect(existsSync(dbPath1)).toBe(false);
				expect(existsSync(dbPath2)).toBe(false);
				expect(existsSync(readOnlyPath)).toBe(false);
				expect(db1.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
				expect(readOnly.isOpen()).toBe(false);
			}
		));

	it('waits for physical destruction before reopening the same path', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
		});
	}, 15_000);

	it('releases the path gate when physical destruction fails', async () => {
		await runDestroyFixture(destroyFailureFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_FAILURE: '1',
		});
	}, 15_000);
});
