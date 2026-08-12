import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

/**
 * `flush()` / `flushSync()` options, and the settle-exactly-once contract both owe their callers.
 * Coverage here is plumbing and the read-only case; the behavioral half of `allowWriteStall` is
 * not covered anywhere — see the note in `write-buffer-manager-stall.test.ts`.
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
	// default.
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
			// Casts stand in for a plain-JS caller ignoring the type.
			expect(() => db.flushSync('yes' as any)).toThrow(/Flush options must be an object/);
			expect(() => db.flushSync({ allowWriteStall: 'yes' } as any)).toThrow(
				/Flush options must be an object/
			);
			await expect(db.flush('yes' as any)).rejects.toThrow(/Flush options must be an object/);
		}));

	/**
	 * #774. A regression is a hang rather than a failed assertion — the promise never settles and
	 * no in-test timer can surface it — so the per-test timeout IS the assertion.
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

						// Validation runs ahead of the read-only no-op, so the same argument gets
						// the same verdict in either mode.
						expect(() => readOnlyDb.flushSync('yes' as any)).toThrow(
							/Flush options must be an object/
						);
						await expect(readOnlyDb.flush('yes' as any)).rejects.toThrow(
							/Flush options must be an object/
						);
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
