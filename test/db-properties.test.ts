import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
describe('Database Properties', () => {
	it('should get string property from database', () => dbRunner(async ({ db }) => {
		// Put some data to ensure the database has stats
		await db.put('key1', 'value1');
		await db.put('key2', 'value2');
		await db.flush();

		// Get level stats property
		const levelStats = db.getDBProperty('rocksdb.levelstats');
		expect(levelStats).toBeDefined();
		expect(typeof levelStats).toBe('string');
		expect(levelStats.length).toBeGreaterThan(0);
	}));

	it('should get stats property from database', () => dbRunner(async ({ db }) => {
		// Put some data
		await db.put('foo', 'bar');
		await db.flush();

		// Get stats property
		const stats = db.getDBProperty('rocksdb.stats');
		expect(stats).toBeDefined();
		expect(typeof stats).toBe('string');
		expect(stats.length).toBeGreaterThan(0);
	}));

	it('should get integer property from database', () => dbRunner(async ({ db }) => {
		// Put some data
		await db.put('key1', 'value1');
		await db.put('key2', 'value2');

		// Get estimated number of keys
		const numKeys = db.getDBIntProperty('rocksdb.estimate-num-keys');
		expect(numKeys).toBeDefined();
		expect(typeof numKeys).toBe('number');
		expect(numKeys).toBeGreaterThan(0);
	}));

	it('should get num-files-at-level property', () => dbRunner(async ({ db }) => {
		// Put some data and flush to create files
		await db.put('key1', 'value1');
		await db.put('key2', 'value2');
		await db.flush();

		// Get number of files at level 0, for some reason this is a string property
		const numFiles = +db.getDBProperty('rocksdb.num-files-at-level0');
		expect(numFiles).toBeDefined();
		expect(typeof numFiles).toBe('number');
		expect(numFiles).toBeGreaterThan(0);
	}));

	it('should test blob files with num-blob-files property', () => dbRunner(async ({ db }) => {
		// Create large values that should trigger blob storage
		// RocksDB typically uses blobs for values larger than a threshold (often 1KB or configurable)
		const largeValue = 'x'.repeat(10000); // 10KB value to ensure blob creation

		// Write multiple large values
		await db.put('blob1', largeValue);
		await db.put('blob2', largeValue);
		await db.put('blob3', largeValue);
		await db.flush();

		// Get number of blob files
		const numBlobFiles = db.getDBIntProperty('rocksdb.num-blob-files');
		expect(numBlobFiles).toBeDefined();
		expect(typeof numBlobFiles).toBe('number');
		expect(numBlobFiles).toBeGreaterThan(0);

		// Note: Whether blobs are actually created depends on RocksDB configuration
		// The test verifies the method works, not necessarily that blobs are created
	}));

	it('should get live-blob-file-size property', () => dbRunner(async ({ db }) => {
		// Create large values
		const largeValue = 'x'.repeat(10000);
		await db.put('blob1', largeValue);
		await db.put('blob2', largeValue);
		await db.flush();

		// Get live blob file size
		const blobSize = db.getDBIntProperty('rocksdb.live-blob-file-size');
		expect(blobSize).toBeDefined();
		expect(typeof blobSize).toBe('number');
		expect(blobSize).toBeGreaterThan(0);
	}));

	it('should get total-blob-file-size property', () => dbRunner(async ({ db }) => {
		// Create large values
		const largeValue = 'x'.repeat(10000);
		await db.put('blob1', largeValue);
		await db.flush();

		// Get total blob file size
		const totalBlobSize = db.getDBIntProperty('rocksdb.total-blob-file-size');
		expect(totalBlobSize).toBeDefined();
		expect(typeof totalBlobSize).toBe('number');
		expect(totalBlobSize).toBeGreaterThan(0);
	}));

	it('should get background-errors property', () => dbRunner(async ({ db }) => {
		const bgErrors = db.getDBIntProperty('rocksdb.background-errors');
		expect(bgErrors).toBeDefined();
		expect(typeof bgErrors).toBe('number');
		expect(bgErrors).toBe(0); // Should be 0 for a healthy database
	}));

	it('should get cur-size-all-mem-tables property', () => dbRunner(async ({ db }) => {
		await db.put('key1', 'value1');
		await db.put('key2', 'value2');

		const memTableSize = db.getDBIntProperty('rocksdb.cur-size-all-mem-tables');
		expect(memTableSize).toBeDefined();
		expect(typeof memTableSize).toBe('number');
		expect(memTableSize).toBeGreaterThan(0);
	}));

	it('should throw error for invalid string property', () => dbRunner(async ({ db }) => {
		expect(() => {
			db.getDBProperty(undefined as any);
		}).toThrow('Property name is required');

		expect(() => {
			db.getDBProperty('invalid.property.name.that.does.not.exist');
		}).toThrow('Failed to get database property');
	}));

	it('should throw error for invalid integer property', () => dbRunner(async ({ db }) => {
		expect(() => {
			db.getDBIntProperty('invalid.property.name.that.does.not.exist');
		}).toThrow('Failed to get database integer property');
	}));

	it('should throw error when database is not open', () => dbRunner({ skipOpen: true }, async ({ db }) => {
		expect(() => {
			db.getDBProperty('rocksdb.stats');
		}).toThrow('Database not open');

		expect(() => {
			db.getDBIntProperty('rocksdb.estimate-num-keys');
		}).toThrow('Database not open');
	}));
});
