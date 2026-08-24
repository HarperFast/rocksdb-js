import { RocksDatabase, registryStatus, shutdown } from '../src/index.ts';
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
const flushFailureFixture = join(__dirname, 'fixtures', 'fork-flush-failure.mts');
const backupDestroyFixture = join(__dirname, 'fixtures', 'fork-backup-destroy.mts');
const iteratorNextRaceFixture = join(__dirname, 'fixtures', 'fork-iterator-next-race.mts');
const countDestroyRaceFixture = join(__dirname, 'fixtures', 'fork-count-destroy-race.mts');
const nodeExecutable =
	process.env.NODE_BINARY ??
	(process.versions.bun || process.versions.deno
		? (process.env.npm_node_execpath ?? 'node')
		: process.execPath);

function runDestroyFixture(
	fixture: string,
	dbPath: string,
	env?: NodeJS.ProcessEnv
): Promise<void> {
	return new Promise((resolve, reject) => {
		// These fixtures depend on Node's type stripping, GC flag, and worker semantics.
		const child = spawn(nodeExecutable, ['--expose-gc', fixture, dbPath], {
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
		child.on('error', (error) => {
			clearTimeout(timeout);
			reject(new Error(`Unable to run lifecycle fixture with Node (${nodeExecutable}): ${error}`));
		});
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
		expect(() => RocksDatabase.config({ lifecycleWaitSeconds: undefined })).not.toThrow();
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

	it('should reject destroy from a never-opened handle', () =>
		dbRunner(({ db, dbPath }) => {
			db.close();
			expect(() => new RocksDatabase(dbPath, { readOnly: true }).destroy()).toThrow(
				'Unsupported operation in read-only mode'
			);
			expect(() => new RocksDatabase(dbPath).destroy()).toThrow(
				'Database path is required for destroy'
			);
			expect(existsSync(dbPath)).toBe(true);
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
		}));

	it('should destroy an open database', () =>
		dbRunner(({ db, dbPath }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
			expect(db.isOpen()).toBe(false);
		}));

	it('waits for an in-flight directory backup before destroying', async () => {
		await runDestroyFixture(backupDestroyFixture, generateDBPath(), {
			ROCKSDB_JS_BACKUP_DELAY_MS: '500',
		});
	});

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

	it.skipIf(process.platform === 'win32' || (process.getuid?.() ?? 0) === 0)(
		'quarantines a path when post-destroy cleanup fails',
		() =>
			dbRunner(async ({ db, dbPath }) => {
				const healthyPath = generateDBPath();
				const healthy = RocksDatabase.open(healthyPath);
				let resolveCloseFailure: (args: unknown[]) => void;
				const closeFailure = new Promise<unknown[]>((resolve) => {
					resolveCloseFailure = resolve;
				});
				const listener = (...args: unknown[]) => {
					if (args[0] === dbPath) resolveCloseFailure(args);
				};
				RocksDatabase.on('database:closeFailed', listener);
				healthy.putSync('key', 'value');
				const lockedDirectory = join(dbPath, 'transaction_logs', 'locked');
				mkdirSync(lockedDirectory, { recursive: true });
				writeFileSync(join(lockedDirectory, 'leftover'), 'data');
				chmodSync(lockedDirectory, 0o000);
				try {
					expect(() => db.destroy()).toThrow('Failed to remove database directory');
					expect(
						registryStatus().find((entry) => entry.path === dbPath)?.destroyCleanupPending
					).toBe(true);
					expect(() => RocksDatabase.open(dbPath)).toThrow('previous destroy cleanup failed');
					await expect(closeFailure).resolves.toMatchObject([
						dbPath,
						expect.stringContaining('Failed to remove database directory'),
					]);
					chmodSync(lockedDirectory, 0o700);
					// shutdown() is deliberately non-destructive: it must not retry
					// path deletion (only an explicit destroy() call may), so a
					// pending tombstone does not make it throw, and it does not
					// clear the tombstone even though the underlying cause is fixed.
					shutdown();
					expect(existsSync(dbPath)).toBe(true);
					expect(
						registryStatus().find((entry) => entry.path === dbPath)?.destroyCleanupPending
					).toBe(true);
				} finally {
					RocksDatabase.off('database:closeFailed', listener);
					if (existsSync(lockedDirectory)) chmodSync(lockedDirectory, 0o700);
				}
				db.destroy();
				expect(registryStatus().some((entry) => entry.path === dbPath)).toBe(false);
				const healthyReopened = RocksDatabase.open(healthyPath);
				expect(healthyReopened.getSync('key')).toBe('value');
				healthyReopened.destroy();
				const reopened = RocksDatabase.open(dbPath);
				reopened.close();
			})
	);

	it('waits for physical destruction before reopening the same path', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
		});
	}, 15_000);

	it('closes an iterator safely when destroy races its construction', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
			ROCKSDB_JS_ITERATOR_SETUP_DELAY_MS: '250',
			ROCKSDB_JS_TEST_ITERATOR_DESTROY_RACE: '1',
		});
	}, 15_000);

	it('serializes an in-progress Next() against a foreign forced close', async () => {
		// Unlike the constructor race above, this positions the destroy while
		// a Next() call already holds iteratorMutex, so it must block on the
		// mutex rather than racing it -- the actual case iteratorMutex exists
		// for. See fork-iterator-next-race.mts.
		await runDestroyFixture(iteratorNextRaceFixture, generateDBPath(), {
			ROCKSDB_JS_ITERATOR_NEXT_DELAY_MS: '250',
		});
	}, 15_000);

	it('aborts an in-flight getCount() when a foreign destroy begins', async () => {
		// getCount() scans the whole range under one OperationGuard, which
		// finishClose() drains with an untimed wait. See fork-count-destroy-race.mts.
		await runDestroyFixture(countDestroyRaceFixture, generateDBPath(), {
			ROCKSDB_JS_COUNT_DELAY_MS: '50',
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

	it('quarantines a flush failure until shutdown preserves the unflushed data', async () => {
		await runDestroyFixture(flushFailureFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '1',
		});
	}, 15_000);

	it('waits for an in-progress shutdown retry before reopening', async () => {
		await runDestroyFixture(shutdownRetryFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
			ROCKSDB_JS_CLOSE_RETRY_DELAY_MS: '1000',
		});
	}, 15_000);
});
