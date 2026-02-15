import { versions } from '../src/index.js';
import { describe, expect, it } from 'vitest';

describe('versions', () => {
	it('should get the versions', () => {
		expect(versions).toHaveProperty('rocksdb');
		expect(versions.rocksdb).toMatch(/^\d+\.\d+\.\d+$/);
		expect(versions).toHaveProperty('rocksdb-js');
		expect(versions['rocksdb-js']).toBe('ROCKSDB_JS_VERSION');
	});
});
