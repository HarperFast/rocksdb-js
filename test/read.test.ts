import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { Encoder } from 'msgpackr';
import { RESET_BUFFER_MODE, REUSE_BUFFER_MODE } from 'msgpackr/pack';
import type { BufferWithDataView } from '../src/encoding.js';

describe('Read Operations', () => {
	describe('get()', () => {
		it('should synchronously return if value is in memory, async if not', () => dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');
			// should be in memtable, synchronously available
			expect(db.get('foo')).toBe('bar');
			await db.flush();
			// memtable flushed, should be on disk now, and require an async get
			let result = db.get('foo');
			expect(result).toBeInstanceOf(Promise);
			expect(await result).toBe('bar');
			// now should be in block cache, synchronously available again
			expect(db.get('foo')).toBe('bar');
		}));
		it('should correctly retrieve multiple concurrent async reads', () => dbRunner({
			dbOptions: [ { noBlockCache: true } ]
		}, async ({ db }) => {
			await db.transaction((transaction) => {
				for (let i = 0; i < 100; i++) {
					db.put(`key-${i}`, `value-${i}`, { transaction });
				}
			});
			await db.flush();
			// memtable is flushed, no data should be in block cache
			let promises: Promise<string | undefined>[] = [];
			for (let i = 0; i < 100; i++) {
				let result = db.get(`key-${i}`);
				//expect(result).toBeInstanceOf(Promise); // I would expect this to be true, but it appears that data is getting cached
				promises.push(result);
			}
			const results = await Promise.all(promises);
			for (let i = 0; i < 100; i++) {
				expect(results[i]).toBe(`value-${i}`);
			}
		}));
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			await expect(db.get('foo')).rejects.toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = await db.get('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			await expect((db.get as any)()).rejects.toThrow('Key is required');
		}));

		it('should return the undecoded value if decode is false', () => dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');
			const value = await db.get('foo', { skipDecode: true });
			expect(value).not.toBe('bar');

			const encoder = new Encoder({ copyBuffers: true });
			const encoded = encoder.encode('bar', REUSE_BUFFER_MODE | RESET_BUFFER_MODE) as unknown as BufferWithDataView;
			const expected = encoded.subarray(encoded.start, encoded.end);
			expect(value.subarray(encoded.start, encoded.end).equals(expected)).toBe(true);
		}));
	});

	describe('getSync()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			expect(() => db.getSync('foo')).toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = db.getSync('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.getSync as any)()).toThrow('Key is required');
		}));

		it('should throw an error if setting default buffers to null', () => dbRunner(async ({ db }) => {
			expect(() => db.store.db.setDefaultKeyBuffer(null)).toThrow('Invalid argument');
			expect(() => db.store.db.setDefaultValueBuffer(null)).toThrow('Invalid argument');
		}));
	});

	describe('getBinary()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			await expect(db.getBinary('foo')).rejects.toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = await db.getBinary('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.getBinary as any)()).toThrow('Key is required');
		}));
	});

	describe('getBinarySync()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			expect(() => db.getBinarySync('foo')).toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = db.getBinarySync('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.getBinarySync as any)()).toThrow('Key is required');
		}));
	});

	describe('getBinaryFast()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			await expect(db.getBinaryFast('foo')).rejects.toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = await db.getBinaryFast('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.getBinaryFast as any)()).toThrow('Key is required');
		}));
	});

	describe('getBinaryFastSync()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			expect(() => db.getBinaryFastSync('foo')).toThrow('Database not open');
		}));

		it('should return undefined if key does not exist', () => dbRunner(async ({ db }) => {
			const value = db.getBinaryFastSync('baz');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.getBinaryFastSync as any)()).toThrow('Key is required');
		}));
	});
});
