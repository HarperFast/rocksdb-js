import { describe, it } from 'vitest';
import { RocksDatabase } from '../src/database.js';
import { generateDBPath } from './lib/util.js';
import { expect } from 'vitest';
import { rimraf } from 'rimraf';

describe('Encoder', () => {
	it('should support custom encoder class', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		class CustomEncoder {
			encode(value: any) {
				return Buffer.from(value);
			}

			decode(value: Buffer) {
				return value.toString();
			}
		}

		try {
			db = await RocksDatabase.open(dbPath, {
				encoder: {
					Encoder: CustomEncoder
				}
			});

			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should support custom encode function', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, {
				encoder: {
					encode: (value: any) => Buffer.from(value)
				}
			});

			await db.put('foo', 'bar');
			const value: Buffer = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should encode using binary encoding', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: 'binary'
			});
			
			await db.put('foo', 'bar');
			const value: Buffer = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should encode using ordered-binary encoding', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: 'ordered-binary'
			});
			
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should encode using msgpack encoding', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: 'msgpack'
			});
			
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should ensure encoded values are buffers', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();
		
		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: 'binary',
				encoder: {
					encode: (value: any) => value
				}
			});
			
			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBeInstanceOf(Buffer);
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should disable encoding', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: false
			});
			
			await db.put('foo', Buffer.from('bar'));
			const value: Buffer = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should error encoding an unsupported value', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();
		
		try {	
			db = await RocksDatabase.open(dbPath, {
				encoding: false
			});

			await expect(db.put('foo', () => {})).rejects.toThrow('Invalid value put in database (function), consider using an encoder');
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});

	it('should decode using readKey', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();
		
		try {
			db = await RocksDatabase.open(dbPath, {
				encoding: false,
				encoder: {
					decode: null as any,
					readKey: (buffer: Buffer, start: number, end?: number) => buffer.subarray(start, end)
				}
			});

			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value.equals(Buffer.from('bar'))).toBe(true);
		} finally {
			await db?.close();
			await rimraf(dbPath);
		}
	});
});
