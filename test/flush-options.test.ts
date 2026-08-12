import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

/**
 * `flush()` / `flushSync()` options, and the promise-settling contract both owe their callers.
 *
 * The behavioral half of `allowWriteStall` — that a default flush *waits out* a write-stall
 * condition while `allowWriteStall: true` proceeds through it — is exercised in
 * `write-buffer-manager-stall.test.ts`, which is the only file that builds a stalling
 * WriteBufferManager (the singleton's `allowStall` is fixed at creation, so it needs its own
 * process). What is covered here is the plumbing and the read-only case.
 */
describe('flush options', () => {
	it('should accept allowWriteStall and still flush durably', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', 'one');
			await expect(db.flush({ allowWriteStall: true })).resolves.toBeUndefined();
			expect(await db.get('a')).toBe('one');
		}));

	it('should accept allowWriteStall on the sync flush', () =>
		dbRunner(async ({ db }) => {
			await db.put('b', 'two');
			expect(() => db.flushSync({ allowWriteStall: true })).not.toThrow();
			expect(await db.get('b')).toBe('two');
		}));

	// The option is additive: every existing caller passes nothing and must keep the RocksDB
	// default (`allow_write_stall: false`).
	it('should flush with no options, and with an empty or partial bag', () =>
		dbRunner(async ({ db }) => {
			await db.put('c', 'three');
			await expect(db.flush()).resolves.toBeUndefined();
			await expect(db.flush({})).resolves.toBeUndefined();
			await expect(db.flush({ allowWriteStall: false })).resolves.toBeUndefined();
			db.flushSync();
			expect(await db.get('c')).toBe('three');
		}));

	it('should reject a non-object options argument', () =>
		dbRunner(async ({ db }) => {
			// Cast: the point is what happens when the type is ignored (plain JS callers).
			expect(() => db.flushSync('yes' as any)).toThrow(/Flush options must be an object/);
			expect(() => db.flushSync({ allowWriteStall: 'yes' } as any)).toThrow(
				/Flush options must be an object/
			);
			await expect(db.flush('yes' as any)).rejects.toThrow(/Flush options must be an object/);
		}));

	/**
	 * HarperFast/rocksdb-js#774: `Flush`/`Compact` used to return without invoking EITHER callback
	 * on a read-only database, so the promise never settled. A regression is a hang, not a failed
	 * assertion, so each of these carries its own timeout — that timeout is the assertion.
	 */
	describe('read-only databases settle their promise', () => {
		it(
			'should resolve flush() rather than hang',
			() =>
				dbRunner(
					{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
					async ({ db }, { db: readOnlyDb }) => {
						db.open();
						await db.put('d', 'four');
						readOnlyDb.open();
						expect(readOnlyDb.readOnly).toBe(true);

						await expect(readOnlyDb.flush()).resolves.toBeUndefined();
						await expect(readOnlyDb.flush({ allowWriteStall: true })).resolves.toBeUndefined();
						expect(() => readOnlyDb.flushSync()).not.toThrow();
					}
				),
			10_000
		);

		it(
			'should resolve compact() rather than hang',
			() =>
				dbRunner(
					{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
					async ({ db }, { db: readOnlyDb }) => {
						db.open();
						await db.put('e', 'five');
						readOnlyDb.open();

						await expect(readOnlyDb.compact()).resolves.toBeUndefined();
					}
				),
			10_000
		);
	});
});
