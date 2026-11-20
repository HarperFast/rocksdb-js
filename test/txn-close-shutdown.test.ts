import { runChild } from './lib/child-process.ts';
import { generateDBPath } from './lib/util.ts';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

async function expectFixturePasses(
	fixture: string,
	args: string[],
	overrides: Record<string, string | undefined>,
	nodeArgs: string[] = []
): Promise<void> {
	const env: NodeJS.ProcessEnv = { ...process.env, ...overrides };
	for (const [name, value] of Object.entries(overrides)) {
		if (value === undefined) delete env[name];
	}
	const { code, signal, stderr, timedOut } = await runChild(
		process.execPath,
		[...nodeArgs, join(__dirname, 'fixtures', fixture), generateDBPath(), ...args],
		{ deadlineMs: 20_000, env }
	);
	expect({ code, signal, stderr, timedOut }).toEqual({
		code: 0,
		signal: null,
		stderr: '',
		timedOut: false,
	});
}

describe.skipIf(!!process.versions.deno)('Transaction close() vs. foreign shutdown()', () => {
	// Collects the wrapper with Node's --expose-gc.
	it.skipIf(!!process.versions.bun)(
		'waits for an in-flight orphan close',
		{ timeout: 30_000 },
		() =>
			expectFixturePasses(
				'fork-txn-close-shutdown.mts',
				['orphan'],
				{ ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH: '1' },
				['--expose-gc']
			)
	);

	it.each(['abort', 'commit-sync'])(
		'waits for an in-flight %s close',
		{ timeout: 30_000 },
		(mode) =>
			expectFixturePasses('fork-txn-close-shutdown.mts', [mode], {
				ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH: '1',
			})
	);

	it.each([
		{ label: 'default', mode: undefined },
		{ label: '2', mode: '2' },
		{ label: '0', mode: '0' },
	])(
		'releases a committed transaction on a quarantined database (ROCKSDB_JS_COMMIT_THREAD=$label)',
		{ timeout: 30_000 },
		({ mode }) =>
			expectFixturePasses('fork-commit-quarantine-close.mts', [], {
				ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '1',
				ROCKSDB_JS_COMMIT_THREAD: mode,
			})
	);
});
