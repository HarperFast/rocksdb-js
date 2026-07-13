import { fileLockRelease, tryFileLock } from '../src/index.js';
import { createWorkerBootstrapScript, generateDBPath, terminateWorker } from './lib/util.js';
import { chmodSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it } from 'vitest';

const tempDirs: string[] = [];

function tempDir(): string {
	const dir = generateDBPath();
	tempDirs.push(dir);
	return dir;
}

function lockPath(): string {
	const dir = tempDir();
	mkdirSync(dir, { recursive: true });
	return join(dir, '.test.lock');
}

describe('File Lock', () => {
	afterEach(() => {
		for (const dir of tempDirs) {
			rmSync(dir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempDirs.length = 0;
	});

	describe('tryFileLock()', () => {
		it('should return a non-zero token and create the lock file', () => {
			const file = lockPath();
			const token = tryFileLock(file);
			try {
				expect(token).toBeGreaterThan(0);
				expect(existsSync(file)).toBe(true);
			} finally {
				fileLockRelease(token);
			}
		});

		it('should return 0 when another holder has the lock', () => {
			const file = lockPath();
			const token = tryFileLock(file);
			try {
				expect(tryFileLock(file)).toBe(0);
			} finally {
				fileLockRelease(token);
			}
		});

		it('should allow re-acquire after release', () => {
			const file = lockPath();
			const token1 = tryFileLock(file);
			fileLockRelease(token1);

			const token2 = tryFileLock(file);
			try {
				expect(token2).toBeGreaterThan(0);
			} finally {
				fileLockRelease(token2);
			}
		});

		it('should allow independent locks on different files', () => {
			const file1 = lockPath();
			const file2 = lockPath();
			const token1 = tryFileLock(file1);
			const token2 = tryFileLock(file2);
			try {
				expect(token1).toBeGreaterThan(0);
				expect(token2).toBeGreaterThan(0);
			} finally {
				fileLockRelease(token1);
				fileLockRelease(token2);
			}
		});

		it('should create missing parent directories', () => {
			const file = join(tempDir(), 'missing', 'nested', 'lock');
			const token = tryFileLock(file);
			try {
				expect(token).toBeGreaterThan(0);
				expect(existsSync(file)).toBe(true);
			} finally {
				fileLockRelease(token);
			}
		});

		it('should allow multiple shared holders on the same file', () => {
			const file = lockPath();
			const token1 = tryFileLock(file, true);
			const token2 = tryFileLock(file, true);
			try {
				expect(token1).toBeGreaterThan(0);
				expect(token2).toBeGreaterThan(0);
			} finally {
				fileLockRelease(token1);
				fileLockRelease(token2);
			}
		});

		it('should reject an exclusive acquire while a shared holder exists', () => {
			const file = lockPath();
			const shared = tryFileLock(file, true);
			try {
				expect(shared).toBeGreaterThan(0);
				expect(tryFileLock(file)).toBe(0);
			} finally {
				fileLockRelease(shared);
			}

			// Once the shared holder releases, the exclusive acquire succeeds.
			const exclusive = tryFileLock(file);
			try {
				expect(exclusive).toBeGreaterThan(0);
			} finally {
				fileLockRelease(exclusive);
			}
		});

		it('should reject a shared acquire while an exclusive holder exists', () => {
			const file = lockPath();
			const exclusive = tryFileLock(file);
			try {
				expect(exclusive).toBeGreaterThan(0);
				expect(tryFileLock(file, true)).toBe(0);
			} finally {
				fileLockRelease(exclusive);
			}
		});

		// A shared (reader) lock must not need write access — a restore reads a
		// backup directory that may be mounted read-only. Opening read-only lets a
		// shared acquire succeed against an already-created lock file whose write bit
		// is stripped, where the old write-always open would have failed with EACCES.
		it.skipIf(process.platform === 'win32')(
			'should acquire a shared lock on a read-only lock file',
			() => {
				const file = lockPath();
				fileLockRelease(tryFileLock(file)); // create the file
				chmodSync(file, 0o444);
				const shared = tryFileLock(file, true);
				try {
					expect(shared).toBeGreaterThan(0);
				} finally {
					fileLockRelease(shared);
					chmodSync(file, 0o644); // let afterEach clean up
				}
			}
		);

		// On a genuinely read-only backup directory no exclusive holder can exist,
		// so a shared lock protects nothing: rather than hard-fail the restore, the
		// shared path degrades to a no-op "acquired". Exclusive still hard-fails.
		// Skipped as root, which bypasses the directory permission bits.
		it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
			'should degrade a shared lock to a no-op on a read-only directory',
			() => {
				const dir = tempDir();
				mkdirSync(dir, { recursive: true });
				const file = join(dir, '.backup.lock'); // does not exist yet
				chmodSync(dir, 0o555); // read-only: the lock file cannot be created
				try {
					const token = tryFileLock(file, true);
					expect(token).toBeGreaterThan(0); // degraded no-op acquire
					expect(existsSync(file)).toBe(false); // nothing was conjured
					expect(() => fileLockRelease(token)).not.toThrow();

					// Exclusive acquisition has no such degrade — it must hard-fail.
					expect(() => tryFileLock(file)).toThrow();
				} finally {
					chmodSync(dir, 0o755); // let afterEach clean up
				}
			}
		);

		it('should acquire a lock on a non-ASCII path', () => {
			const dir = join(tmpdir(), 'rocksdb-js-tests', 'café-répertoire-バックアップ');
			mkdirSync(dir, { recursive: true });
			tempDirs.push(dir);
			const file = join(dir, '.test.lock');

			const token = tryFileLock(file);
			try {
				expect(token).toBeGreaterThan(0);
				expect(existsSync(file)).toBe(true);
				expect(tryFileLock(file)).toBe(0);
			} finally {
				fileLockRelease(token);
			}
		});
	});

	describe('fileLockRelease()', () => {
		it('should be a no-op for token 0', () => {
			expect(() => fileLockRelease(0)).not.toThrow();
		});

		it('should be a no-op for an unknown token', () => {
			expect(() => fileLockRelease(999_999)).not.toThrow();
		});

		it('should release the lock so another acquire succeeds', () => {
			const file = lockPath();
			const token = tryFileLock(file);
			fileLockRelease(token);

			const token2 = tryFileLock(file);
			try {
				expect(token2).toBeGreaterThan(0);
			} finally {
				fileLockRelease(token2);
			}
		});
	});

	it('should exclude across worker_threads', async () => {
		const file = lockPath();
		const token = tryFileLock(file);
		expect(token).toBeGreaterThan(0);

		const worker = new Worker(createWorkerBootstrapScript('./test/workers/file-lock-worker.mts'), {
			eval: true,
			workerData: { file },
		});

		try {
			// While the main thread holds the lock, the worker cannot acquire it.
			await new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', (event) => {
					try {
						if (event.ready) {
							worker.postMessage({ tryAcquire: true });
						} else if (event.tryAcquire !== undefined) {
							expect(event.tryAcquire).toBe(0);
							resolve();
						}
					} catch (err) {
						reject(err);
					}
				});
			});

			fileLockRelease(token);

			// After release, the worker can acquire the lock.
			const workerToken = await new Promise<number>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', (event) => {
					try {
						if (event.acquired !== undefined) {
							resolve(event.acquired);
						}
					} catch (err) {
						reject(err);
					}
				});
				worker.postMessage({ acquire: true });
			});
			expect(workerToken).toBeGreaterThan(0);

			// While the worker holds the lock, the main thread cannot acquire it.
			expect(tryFileLock(file)).toBe(0);

			await new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', (event) => {
					try {
						if (event.released) {
							resolve();
						}
					} catch (err) {
						reject(err);
					}
				});
				worker.postMessage({ release: workerToken });
			});
		} finally {
			fileLockRelease(token);
			worker.postMessage({ close: true });
			await terminateWorker(worker);
		}
	});
});
