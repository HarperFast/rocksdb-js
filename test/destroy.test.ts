import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const destroyOpenFixture = join(__dirname, 'fixtures', 'fork-destroy-open.mts');
const destroyFailureFixture = join(__dirname, 'fixtures', 'fork-destroy-failure.mts');
const closeFailureFixture = join(__dirname, 'fixtures', 'fork-close-failure.mts');
const gcCloseFailureFixture = join(__dirname, 'fixtures', 'fork-gc-close-failure.mts');
const shutdownFailureFixture = join(__dirname, 'fixtures', 'fork-shutdown-failure.mts');
const shutdownRetryFixture = join(__dirname, 'fixtures', 'fork-shutdown-retry.mts');

function runDestroyFixture(
	fixture: string,
	dbPath: string,
	env?: NodeJS.ProcessEnv
): Promise<void> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ['--expose-gc', fixture, dbPath], {
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
		expect(() => RocksDatabase.config({ lifecycleWaitSeconds: 1.5 })).toThrow(
			'Lifecycle wait seconds must be a positive integer'
		);
		expect(() => RocksDatabase.config({ lifecycleWaitSeconds: '30' as unknown as number })).toThrow(
			'Lifecycle wait seconds must be a number'
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

	it.skipIf(process.platform === 'win32')(
		'quarantines a path when post-destroy cleanup fails',
		() =>
			dbRunner(({ db, dbPath }) => {
				const lockedDirectory = join(dbPath, 'transaction_logs', 'locked');
				mkdirSync(lockedDirectory, { recursive: true });
				writeFileSync(join(lockedDirectory, 'leftover'), 'data');
				chmodSync(lockedDirectory, 0o000);
				try {
					expect(() => db.destroy()).toThrow('Failed to remove database directory');
					expect(() => RocksDatabase.open(dbPath)).toThrow('previous destroy cleanup failed');
				} finally {
					chmodSync(lockedDirectory, 0o700);
				}
				db.destroy();
				const reopened = RocksDatabase.open(dbPath);
				reopened.close();
			})
	);

	it('waits for physical destruction before reopening the same path', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
		});
	}, 15_000);

	it('waits for physical destruction before shutdown completes', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
			ROCKSDB_JS_TEST_SHUTDOWN_DURING_DESTROY: '1',
		});
	}, 15_000);

	it('releases the path gate when physical destruction fails', async () => {
		await runDestroyFixture(destroyFailureFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_FAILURE: '1',
		});
	}, 15_000);

	it('quarantines a descriptor whose native close fails', async () => {
		await runDestroyFixture(destroyFailureFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
		});
	}, 15_000);

	it('surfaces shutdown close failures and quarantines the whole path', async () => {
		await runDestroyFixture(shutdownFailureFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
		});
	}, 15_000);

	it('quarantines a failed automatic last-handle close', async () => {
		await runDestroyFixture(gcCloseFailureFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
		});
	}, 15_000);

	it('surfaces an explicit close failure and permits a shutdown retry', async () => {
		await runDestroyFixture(closeFailureFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
		});
	}, 15_000);

	it('waits for an in-progress shutdown retry before reopening', async () => {
		await runDestroyFixture(shutdownRetryFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
		});
	}, 15_000);
});
