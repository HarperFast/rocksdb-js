import { parseDuration, when, withResolvers } from '../src/util.js';
import { describe, expect, it } from 'vitest';

describe('Util', () => {
	describe('parseDuration()', () => {
		it('should parse a duration from a string', () => {
			expect(parseDuration('1s')).toBe(1000);
			expect(parseDuration('1m')).toBe(60000);
			expect(parseDuration('1h')).toBe(3600000);
			expect(parseDuration('1d')).toBe(86400000);
			expect(parseDuration('1ms')).toBe(1);
			expect(parseDuration('1s 1ms')).toBe(1001);
			expect(parseDuration('1m 1s')).toBe(61000);
			expect(parseDuration('1m 1')).toBe(60001);
		});

		it('should throw an error if the string duration is invalid', () => {
			expect(() => parseDuration('1a')).toThrow('Invalid duration: 1a');
			expect(() => parseDuration('1s 1a')).toThrow('Invalid duration: 1s 1a');
			expect(() => parseDuration('h')).toThrow('Invalid duration: h');
			expect(() => parseDuration('foo')).toThrow('Invalid duration: foo');
		});

		it('should parse a duration from a number', () => {
			expect(parseDuration(1000)).toBe(1000);
			expect(parseDuration(60000)).toBe(60000);
			expect(parseDuration(3600000)).toBe(3600000);
			expect(parseDuration(86400000)).toBe(86400000);
		});

		it('should throw an error if the number duration is invalid', () => {
			expect(() => parseDuration(NaN)).toThrow('Invalid duration: NaN');
			expect(() => parseDuration(Infinity)).toThrow('Invalid duration: Infinity');
			expect(() => parseDuration(-Infinity)).toThrow('Invalid duration: -Infinity');
		});
	});

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
			const result = when('foo', (value) => value.toUpperCase());
			expect(result).toBe('FOO');
		});

		it('should call callback that returns a promise', async () => {
			const result = await when(Promise.resolve('foo'), (value) => value.toUpperCase());
			expect(result).toBe('FOO');
		});

		it('should call function that throws error', async () => {
			await expect(
				when(() => {
					throw new Error('foo');
				})
			).rejects.toThrow('foo');
		});

		it('should call function that rejects with error', async () => {
			await expect(when(() => Promise.reject(new Error('foo')))).rejects.toThrow('foo');
		});

		it('should call callback that throws error', async () => {
			await expect(
				when('foo', () => {
					throw new Error('foo');
				})
			).rejects.toThrow('foo');
		});

		it('should call callback that rejects with error', async () => {
			await expect(when('foo', () => Promise.reject(new Error('foo')))).rejects.toThrow('foo');
		});
	});

	describe('withResolvers<void>()', () => {
		it('should resolve with value', async () => {
			const { resolve, promise } = withResolvers<string>();
			resolve('foo');
			await expect(promise).resolves.toBe('foo');
		});

		it('should reject with error', async () => {
			const { reject, promise } = withResolvers<void>();
			reject(new Error('foo'));
			await expect(promise).rejects.toThrow('foo');
		});
	});
});
