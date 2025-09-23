import { describe, expect, it } from 'vitest';
import { versions } from '../src/index.js';

describe('versions', () => {
	it('should get the versions', () => {
		expect(versions).toHaveProperty('rocksdb');
		expect(versions.rocksdb).toMatch(/^\d+\.\d+\.\d+$/);
		expect(versions).toHaveProperty('rocksdb-js');
		expect(versions['rocksdb-js']).toBe('ROCKSDB_JS_VERSION');
	});
});
