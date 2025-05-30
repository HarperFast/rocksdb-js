import { describe, expect, it } from 'vitest';
import { when, withResolvers } from '../src/util.js';

describe('Util', () => {
	describe('when()', () => {
		it('should return a value', () => {
			const result = when('foo');
			expect(result).toBe('foo');
		});

		it('should return a promise', async () => {
			const result = await when(Promise.resolve('foo'));
			expect(result).toBe('foo');
		});

		it('should call function that returns a value', () => {
			const result = when(() => 'foo');
			expect(result).toBe('foo');
		});

		it('should call function that returns a promise', async () => {
			const result = await when(() => Promise.resolve('foo'));
			expect(result).toBe('foo');
		});

		it('should call callback that returns a value', () => {
			const result = when('foo', value => value.toUpperCase());
			expect(result).toBe('FOO');
		});

		it('should call callback that returns a promise', async () => {
			const result = await when(Promise.resolve('foo'), value => value.toUpperCase());
			expect(result).toBe('FOO');
		});

		it('should call function that throws error', async () => {
			await expect(when(() => { throw new Error('foo'); })).rejects.toThrow('foo');
		});

		it('should call function that rejects with error', async () => {
			await expect(when(() => Promise.reject(new Error('foo')))).rejects.toThrow('foo');
		});

		it('should call callback that throws error', async () => {
			await expect(when('foo', () => { throw new Error('foo'); })).rejects.toThrow('foo');
		});

		it('should call callback that rejects with error', async () => {
			await expect(when('foo', () => Promise.reject(new Error('foo')))).rejects.toThrow('foo');
		});
	});

	describe('withResolvers()', () => {
		it('should resolve with value', async () => {
			const { resolve, promise } = withResolvers();
			resolve('foo');
			await expect(promise).resolves.toBe('foo');
		});

		it('should reject with error', async () => {
			const { reject, promise } = withResolvers();
			reject(new Error('foo'));
			await expect(promise).rejects.toThrow('foo');
		});
	});
});
