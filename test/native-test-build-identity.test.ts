import { resolveBuildSelection } from '../scripts/native-test/build-identity.mjs';
import { describe, expect, it } from 'vitest';

describe('native-test runner build selection', () => {
	const pkg = { rocksdb: { version: '11.1.2-2' } };

	it('uses the package pin when no env override is set', () => {
		const { expectedVersion, identity } = resolveBuildSelection({}, pkg, 'darwin');
		expect(expectedVersion).toBe('11.1.2-2');
		expect(identity).toBe('11.1.2-2');
	});

	it('lets ROCKSDB_VERSION override the pin, changing the marker (the reported bug)', () => {
		// Built under an override: the marker must record what was linked (-1), NOT
		// the pin (-2). Otherwise dropping the override later leaves the marker
		// matching the pin and the stale -1 binary is silently reused.
		const under = resolveBuildSelection({ ROCKSDB_VERSION: '11.1.2-1' }, pkg, 'darwin');
		expect(under.expectedVersion).toBe('11.1.2-1');
		expect(under.identity).toBe('11.1.2-1');

		// Dropping the override changes the identity, so the runner rebuilds.
		const without = resolveBuildSelection({}, pkg, 'darwin');
		expect(without.identity).toBe('11.1.2-2');
		expect(under.identity).not.toBe(without.identity);
	});

	it('includes the Linux runtime so a libc change forces a rebuild', () => {
		expect(resolveBuildSelection({}, pkg, 'linux').identity).toBe('11.1.2-2-glibc');
		expect(resolveBuildSelection({ ROCKSDB_LIBC: 'musl' }, pkg, 'linux').identity).toBe(
			'11.1.2-2-musl'
		);
		// Non-Linux has no runtime suffix.
		expect(resolveBuildSelection({}, pkg, 'darwin').identity).toBe('11.1.2-2');
	});

	it('distinguishes a source build from a prebuild', () => {
		const src = resolveBuildSelection({ ROCKSDB_PATH: '/src/rocksdb' }, pkg, 'linux');
		expect(src.identity).toBe('source:/src/rocksdb');
		// Source identity is independent of the runtime/pin so a path change is the
		// only thing that flips it.
		expect(resolveBuildSelection({ ROCKSDB_PATH: '/other' }, pkg, 'linux').identity).toBe(
			'source:/other'
		);
	});
});
