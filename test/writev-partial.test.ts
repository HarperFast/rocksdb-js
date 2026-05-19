import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// Compiles and runs the standalone C++ unit test that exercises
// TransactionLogFile::writevAll's partial-write retry path against a pipe.
// Skipped on Windows; the Win32 writeBatchToFile uses WriteFile, not writev.
describe.skipIf(process.platform === 'win32')('writevAll partial-write retry', () => {
	it('writes all bytes through a pipe even when writev returns short counts', () => {
		const script = join(process.cwd(), 'scripts', 'test-writev-partial.mjs');
		const out = execFileSync(process.execPath, [script], { encoding: 'utf8', timeout: 60_000 });
		expect(out).toContain('OK writev_partial_test');
	});
});
