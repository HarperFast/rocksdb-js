import { RocksDatabase, registryStatus, shutdown } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const destroyOpenFixture = join(__dirname, 'fixtures', 'fork-destroy-open.mts');
const openAttachDestroyFixture = join(__dirname, 'fixtures', 'fork-open-attach-destroy.mts');
const destroyFailureFixture = join(__dirname, 'fixtures', 'fork-destroy-failure.mts');
const closeFailureFixture = join(__dirname, 'fixtures', 'fork-close-failure.mts');
const gcCloseFailureFixture = join(__dirname, 'fixtures', 'fork-gc-close-failure.mts');
const shutdownFailureFixture = join(__dirname, 'fixtures', 'fork-shutdown-failure.mts');
const shutdownRetryFixture = join(__dirname, 'fixtures', 'fork-shutdown-retry.mts');
const foreignCloseLogCacheFixture = join(__dirname, 'fixtures', 'fork-foreign-close-log-cache.mts');
const registryStatusColumnRaceFixture = join(
	__dirname,
	'fixtures',
	'fork-registry-status-column-race.mts'
);
const registryStatusCloseRaceFixture = join(
	__dirname,
	'fixtures',
	'fork-registry-status-close-race.mts'
);
const lifecycleTimeoutFixture = join(__dirname, 'fixtures', 'fork-lifecycle-timeout.mts');
const lifecycleTimeoutTwoDescriptorsFixture = join(
	__dirname,
	'fixtures',
	'fork-lifecycle-timeout-two-descriptors.mts'
);
const flushFailureFixture = join(__dirname, 'fixtures', 'fork-flush-failure.mts');
const backupDestroyFixture = join(__dirname, 'fixtures', 'fork-backup-destroy.mts');
const iteratorNextRaceFixture = join(__dirname, 'fixtures', 'fork-iterator-next-race.mts');
const countDestroyRaceFixture = join(__dirname, 'fixtures', 'fork-count-destroy-race.mts');
const compactCancelSyncFixture = join(__dirname, 'fixtures', 'fork-compact-cancel-sync.mts');
const compactCancelAsyncFixture = join(__dirname, 'fixtures', 'fork-compact-cancel-async.mts');
const compactCancelCloseFixture = join(__dirname, 'fixtures', 'fork-compact-cancel-close.mts');
const compactCancelDestroyFixture = join(__dirname, 'fixtures', 'fork-compact-cancel-destroy.mts');
const quarantinedExitFixture = join(__dirname, 'fixtures', 'fork-quarantined-exit.mts');
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
			const passed = code === 0 && signal === null;
			// The child owns dbPath, and several fixtures deliberately end with a
			// live or quarantined database, so nothing else removes it. Keep it
			// on failure (and under KEEP_FILES) so the state is inspectable.
			if (passed && !process.env.KEEP_FILES) {
				// This runs after the Promise executor returned, so a throw here
				// (Windows EBUSY past maxRetries) would be an uncaught exception
				// that kills the worker rather than a failed test.
				try {
					rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
				} catch {
					// leftover directory only; the fixture itself passed
				}
			}
			if (passed) {
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

	it('should reject destroy from a never-opened handle', () =>
		dbRunner(({ db, dbPath }) => {
			db.close();
			expect(() => new RocksDatabase(dbPath, { readOnly: true }).destroy()).toThrow(
				'Unsupported operation in read-only mode'
			);
			expect(() => new RocksDatabase(dbPath).destroy()).toThrow(
				'Database must be opened before it can be destroyed'
			);
			expect(existsSync(dbPath)).toBe(true);
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
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

	// The macOS `/var` -> `/private/var` case, reproduced with an explicit symlink
	// so it runs everywhere: `registryStatus().path` and `database:closeFailed`
	// must report the spelling the caller opened, not the resolved identity the
	// registry key carries. A tombstone has no descriptor left to ask, so the
	// entry has to remember it.
	it.skipIf(process.platform === 'win32' || (process.getuid?.() ?? 0) === 0)(
		'reports the opened path spelling, not the resolved identity, for a symlinked path',
		async () => {
			const realPath = generateDBPath();
			const linkPath = `${realPath}-link`;
			mkdirSync(realPath, { recursive: true });
			symlinkSync(realPath, linkPath, 'dir');
			const lockedDirectory = join(realPath, 'transaction_logs', 'locked');
			// Filtered to this path so an event from anything else in the file
			// cannot shift the indices asserted below.
			const closeFailures: unknown[][] = [];
			let notify: () => void = () => {};
			const listener = (...args: unknown[]) => {
				if (args[0] === linkPath || args[0] === realPath) closeFailures.push(args);
				notify();
			};
			const nextCloseFailure = (count: number) =>
				new Promise<void>((resolve, reject) => {
					const timer = setTimeout(
						() => reject(new Error(`database:closeFailed #${count} never arrived`)),
						5_000
					);
					notify = () => {
						if (closeFailures.length >= count) {
							clearTimeout(timer);
							resolve();
						}
					};
					notify();
				});
			RocksDatabase.on('database:closeFailed', listener);
			const db = RocksDatabase.open(linkPath);
			try {
				db.putSync('key', 'value');
				expect(registryStatus().find((entry) => entry.path === linkPath)).toBeDefined();
				mkdirSync(lockedDirectory, { recursive: true });
				writeFileSync(join(lockedDirectory, 'leftover'), 'data');
				chmodSync(lockedDirectory, 0o000);
				expect(() => db.destroy()).toThrow('Failed to remove database directory');
				// The descriptor is gone by now, so this is the entry's remembered
				// spelling rather than the live descriptor's.
				expect(
					registryStatus().find((entry) => entry.path === linkPath)?.destroyCleanupPending
				).toBe(true);
				await nextCloseFailure(1);
				expect(closeFailures[0]).toMatchObject([
					linkPath,
					expect.stringContaining('Failed to remove database directory'),
				]);
				// Retrying the destroy finds only the tombstone -- no descriptor to
				// ask -- so the remembered spelling is the only source left.
				expect(() => db.destroy()).toThrow('Failed to remove database directory');
				await nextCloseFailure(2);
				expect(closeFailures[1]).toMatchObject([
					linkPath,
					expect.stringContaining('Failed to remove database directory'),
				]);
			} finally {
				RocksDatabase.off('database:closeFailed', listener);
				if (existsSync(lockedDirectory)) chmodSync(lockedDirectory, 0o700);
				db.destroy();
				rmSync(linkPath, { force: true });
				rmSync(realPath, { force: true, recursive: true });
			}
		}
	);

	it('waits for physical destruction before reopening the same path', async () => {
		await runDestroyFixture(destroyOpenFixture, generateDBPath(), {
			ROCKSDB_JS_DESTROY_DELAY_MS: '2000',
		});
	}, 15_000);

	it('attaches a racing open before destroy can claim its descriptor', async () => {
		await runDestroyFixture(openAttachDestroyFixture, generateDBPath(), {
			ROCKSDB_JS_OPEN_ATTACH_DELAY_MS: '2000',
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

	it("drops a handle's transaction-log cache when it reopens after a foreign shutdown", async () => {
		await runDestroyFixture(foreignCloseLogCacheFixture, generateDBPath());
	}, 20_000);

	it('survives a column-family drop racing a registryStatus() walk', async () => {
		await runDestroyFixture(registryStatusColumnRaceFixture, generateDBPath(), {
			ROCKSDB_JS_REGISTRY_STATUS_COLUMNS_DELAY_MS: '10',
		});
	}, 15_000);

	it('purges the last handle closed during a registryStatus() walk', async () => {
		await runDestroyFixture(registryStatusCloseRaceFixture, generateDBPath(), {
			ROCKSDB_JS_REGISTRY_STATUS_COLUMNS_DELAY_MS: '20',
		});
	}, 15_000);

	it('times out an open past lifecycleWaitSeconds and recovers once the retry finishes', async () => {
		await runDestroyFixture(lifecycleTimeoutFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FAILURE: '1',
			ROCKSDB_JS_CLOSE_RETRY_DELAY_MS: '3000',
		});
	}, 15_000);

	it('reopens promptly after two concurrently-retrying descriptors finish, not at the deadline', async () => {
		await runDestroyFixture(lifecycleTimeoutTwoDescriptorsFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '2',
			ROCKSDB_JS_CLOSE_RETRY_DELAY_MS: '1500',
		});
	}, 15_000);

	it('cancels an in-flight synchronous compaction when destroy claims the descriptor', async () => {
		await runDestroyFixture(compactCancelSyncFixture, generateDBPath(), {
			ROCKSDB_JS_COMPACT_DELAY_MS: '10000',
		});
	}, 20_000);

	it('cancels an in-flight asynchronous compaction when destroy claims the descriptor', async () => {
		await runDestroyFixture(compactCancelAsyncFixture, generateDBPath(), {
			ROCKSDB_JS_COMPACT_DELAY_MS: '10000',
		});
	}, 20_000);

	it('cancels an in-flight asynchronous compaction when the owning handle closes', async () => {
		await runDestroyFixture(compactCancelCloseFixture, generateDBPath(), {
			ROCKSDB_JS_COMPACT_DELAY_MS: '10000',
		});
	}, 20_000);

	it('cancels an in-flight asynchronous compaction before close-time compaction blocks on it', async () => {
		await runDestroyFixture(compactCancelDestroyFixture, generateDBPath(), {
			ROCKSDB_JS_COMPACT_DELAY_MS: '8000',
		});
	}, 20_000);

	it('exits cleanly with a descriptor still quarantined at process exit', async () => {
		await runDestroyFixture(quarantinedExitFixture, generateDBPath(), {
			ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '2',
		});
	}, 15_000);
});
