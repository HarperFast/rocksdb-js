import { describe, it } from 'vitest';
import { expect } from 'vitest';
import type { BufferWithDataView } from '../src/encoding';
import { dbRunner } from './lib/util.js';

describe('Encoder', () => {
	class CustomEncoder {
		encode(value: any) {
			return Buffer.from(value);
		}

		decode(value: BufferWithDataView) {
			return value.subarray(value.start, value.end).toString();
		}
	}

	it('should support custom encoder class', () =>
		dbRunner({ dbOptions: [{ encoder: { Encoder: CustomEncoder } }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		}));

	it('should support custom encode function', () =>
		dbRunner(
			{ dbOptions: [{ encoder: { encode: (value: any) => Buffer.from(value) } }] },
			async ({ db }) => {
				await db.put('foo', 'bar');
				const value: BufferWithDataView = await db.get('foo');
				expect(value.subarray(value.start, value.end).equals(Buffer.from('bar'))).toBe(true);
			}
		));

	it('should encode using binary encoding', () =>
		dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			const value: Buffer = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		}));

	it('should encode using ordered-binary encoding', () =>
		dbRunner({ dbOptions: [{ encoding: 'ordered-binary' }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		}));

	it('should encode using msgpack encoding', () =>
		dbRunner({ dbOptions: [{ encoding: 'msgpack' }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		}));

	it('should ensure encoded values are buffers', () =>
		dbRunner(
			{ dbOptions: [{ encoding: 'binary', encoder: { encode: (value: any) => value } }] },
			async ({ db }) => {
				await db.put('foo', 'bar');
				const value = await db.get('foo');
				expect(value).toBeInstanceOf(Buffer);
				expect(value.subarray(value.start, value.end).equals(Buffer.from('bar'))).toBe(true);
			}
		));

	it('should disable encoding', () =>
		dbRunner({ dbOptions: [{ encoding: false }] }, async ({ db }) => {
			await db.put('foo', Buffer.from('bar'));
			const value: Buffer = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		}));

	it('should error encoding an unsupported value', () =>
		dbRunner({ dbOptions: [{ encoding: false }] }, async ({ db }) => {
			await expect(db.put('foo', () => {})).rejects.toThrow(
				'Invalid value put in database (function), consider using an encoder'
			);
		}));

	it('should decode using readKey', () =>
		dbRunner({
			dbOptions: [{
				encoding: false,
				encoder: {
					decode: null as any,
					readKey: (buffer: Buffer, start: number, end?: number) => buffer.subarray(start, end),
				},
			}],
		}, async ({ db }) => {
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		}));
});
