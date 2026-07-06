import { fileLockRelease, tryFileLock } from '../src/index.js';
import { createWorkerBootstrapScript, generateDBPath, terminateWorker } from './lib/util.js';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
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

		it('should throw when the parent directory does not exist', () => {
			const file = join(tempDir(), 'missing', 'lock');
			expect(() => tryFileLock(file)).toThrow(/does not exist/);
		});

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
