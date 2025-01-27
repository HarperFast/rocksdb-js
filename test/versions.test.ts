import { describe, expect, it } from 'vitest';
import { versions } from '../src/index.js';

describe('versions', () => {
	it('should get the versions', () => {
		expect(versions).toMatchObject({
			rocksdb: /^\d+\.\d+\.\d+$/,
			'rocksdb-js': 'ROCKSDB_JS_VERSION', // note: this is a placeholder
		});
	});
});
