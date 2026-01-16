import { Encoder } from 'msgpackr';
import { RESET_BUFFER_MODE, REUSE_BUFFER_MODE } from 'msgpackr/pack';
import { describe, expect, it } from 'vitest';
import type { BufferWithDataView } from '../src/encoding.js';
import { dbRunner } from './lib/util.js';

describe('Read Operations', () => {
	describe('get()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				await expect(db.get('foo')).rejects.toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = await db.get('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				await expect((db.get as any)()).rejects.toThrow('Key is required');
			}));

		it('should return the undecoded value if decode is false', () =>
			dbRunner(async ({ db }) => {
				await db.put('foo', 'bar');
				const value = await db.get('foo', { skipDecode: true });
				expect(value).not.toBe('bar');

				const encoder = new Encoder({ copyBuffers: true });
				const encoded = encoder.encode(
					'bar',
					REUSE_BUFFER_MODE | RESET_BUFFER_MODE
				) as unknown as BufferWithDataView;
				const expected = encoded.subarray(encoded.start, encoded.end);
				expect(value.equals(expected)).toBe(true);
			}));
	});

	describe('getSync()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getSync('foo')).toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = db.getSync('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.getSync as any)()).toThrow('Key is required');
			}));
	});

	describe('getBinary()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				await expect(db.getBinary('foo')).rejects.toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = await db.getBinary('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.getBinary as any)()).toThrow('Key is required');
			}));
	});

	describe('getBinarySync()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getBinarySync('foo')).toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = db.getBinarySync('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.getBinarySync as any)()).toThrow('Key is required');
			}));
	});

	describe('getBinaryFast()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				await expect(db.getBinaryFast('foo')).rejects.toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = await db.getBinaryFast('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.getBinaryFast as any)()).toThrow('Key is required');
			}));
	});

	describe('getBinaryFastSync()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getBinaryFastSync('foo')).toThrow('Database not open');
			}));

		it('should return undefined if key does not exist', () =>
			dbRunner(async ({ db }) => {
				const value = db.getBinaryFastSync('baz');
				expect(value).toBeUndefined();
			}));

		it('should throw an error if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.getBinaryFastSync as any)()).toThrow('Key is required');
			}));
	});
});
