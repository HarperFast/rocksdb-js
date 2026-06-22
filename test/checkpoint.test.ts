import { RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const tempDirs: string[] = [];

/**
 * Returns a unique, not-yet-created temp directory path that is cleaned up
 * after each test. Suitable as a checkpoint target (RocksDB requires the
 * target to not already exist).
 */
function tempDir(): string {
	const dir = generateDBPath();
	tempDirs.push(dir);
	return dir;
}

async function writeAll(db: RocksDatabase, count: number, prefix = 'value'): Promise<void> {
	for (let i = 0; i < count; ++i) {
		await db.put(`key-${i}`, `${prefix}-${i}`);
	}
	await db.flush();
}

describe('Checkpoints', () => {
	afterEach(() => {
		for (const dir of tempDirs) {
			rmSync(dir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempDirs.length = 0;
	});

	it('should create a checkpoint that reads identically to the source', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 100);

			const checkpointDir = tempDir();
			await expect(db.createCheckpoint(checkpointDir)).resolves.toBeUndefined();
			expect(existsSync(checkpointDir)).toBe(true);

			const checkpoint = new RocksDatabase(checkpointDir);
			checkpoint.open();
			try {
				for (let i = 0; i < 100; ++i) {
					expect(await checkpoint.get(`key-${i}`)).toBe(`value-${i}`);
				}
			} finally {
				checkpoint.close();
			}
		}));

	it('should produce an independent, writable database', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 10);

			const checkpointDir = tempDir();
			await db.createCheckpoint(checkpointDir);

			const checkpoint = new RocksDatabase(checkpointDir);
			checkpoint.open();
			try {
				// Diverge both databases and assert they do not see each other's writes.
				await db.put('only-source', 'source');
				await checkpoint.put('only-checkpoint', 'checkpoint');
				await db.put('key-0', 'source-updated');
				await checkpoint.put('key-0', 'checkpoint-updated');

				expect(await db.get('only-source')).toBe('source');
				expect(await db.get('only-checkpoint')).toBeUndefined();
				expect(await db.get('key-0')).toBe('source-updated');

				expect(await checkpoint.get('only-checkpoint')).toBe('checkpoint');
				expect(await checkpoint.get('only-source')).toBeUndefined();
				expect(await checkpoint.get('key-0')).toBe('checkpoint-updated');
			} finally {
				checkpoint.close();
			}
		}));

	it('should flush unflushed memtable data into the checkpoint', () =>
		dbRunner({ dbOptions: [{ disableWAL: true }] }, async ({ db }) => {
			// No explicit flush — createCheckpoint flushes the memtable by default,
			// so this data must be present in the checkpoint even with the WAL off.
			for (let i = 0; i < 25; ++i) {
				await db.put(`key-${i}`, `value-${i}`);
			}

			const checkpointDir = tempDir();
			await db.createCheckpoint(checkpointDir);

			const checkpoint = new RocksDatabase(checkpointDir);
			checkpoint.open();
			try {
				for (let i = 0; i < 25; ++i) {
					expect(await checkpoint.get(`key-${i}`)).toBe(`value-${i}`);
				}
			} finally {
				checkpoint.close();
			}
		}));

	it('should reject when the target path already exists', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			const checkpointDir = tempDir();
			mkdirSync(checkpointDir, { recursive: true });

			await expect(db.createCheckpoint(checkpointDir)).rejects.toThrow();
		}));

	it('should reject when the parent directory does not exist', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			// Parent ("missing") is never created; RocksDB does not create parents.
			const checkpointDir = join(tempDir(), 'missing', 'checkpoint');

			await expect(db.createCheckpoint(checkpointDir)).rejects.toThrow();
		}));

	it('should not crash when closing during a checkpoint', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			db.open();
			await writeAll(db, 200);

			const checkpointDir = tempDir();
			const checkpointPromise = db.createCheckpoint(checkpointDir);

			// Close while the checkpoint is in flight. The descriptor is kept alive
			// for the duration of the copy, so this must settle cleanly (resolve or
			// reject) rather than crash.
			db.close();

			await expect(
				checkpointPromise.then(
					() => 'settled',
					() => 'settled'
				)
			).resolves.toBe('settled');
		}));

	it('should not free the database under an in-flight checkpoint when destroy() races it', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			db.open();
			await writeAll(db, 200);

			const checkpointDir = tempDir();
			const settled = db.createCheckpoint(checkpointDir).then(
				() => 'settled',
				() => 'settled'
			);

			// destroy() tears down via DBDescriptor::finishClose(), which (unlike
			// close()) bypasses the async-work tracker. The checkpoint registers in
			// operationsInFlight, so finishClose() waits for the copy to finish
			// before resetting descriptor->db — the worker never touches a freed DB.
			// The in-flight op still holds a descriptor reference, so destroy() throws
			// rather than tearing the database down mid-copy.
			expect(() => db.destroy()).toThrow();

			// The checkpoint itself completed (finishClose waited for it), so the
			// promise settles cleanly and nothing crashes.
			await expect(settled).resolves.toBe('settled');
		}));
});
