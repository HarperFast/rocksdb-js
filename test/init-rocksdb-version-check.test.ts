import {
	installedSatisfiesPin,
	isExactVersionPin,
	prebuildIsRedundant,
} from '../scripts/init-rocksdb/version-check.ts';
import { describe, expect, it } from 'vitest';

describe('init-rocksdb version-check', () => {
	describe('isExactVersionPin', () => {
		it('treats a concrete version (incl. revision suffix) as a pin', () => {
			expect(isExactVersionPin('11.1.2')).toBe(true);
			expect(isExactVersionPin('11.1.2-1')).toBe(true);
		});

		it('does not treat "latest" or an unset version as a pin', () => {
			expect(isExactVersionPin('latest')).toBe(false);
			expect(isExactVersionPin(undefined)).toBe(false);
			expect(isExactVersionPin('')).toBe(false);
		});
	});

	describe('installedSatisfiesPin', () => {
		it('is true when the installed version exactly matches the pin', () => {
			expect(installedSatisfiesPin({ version: '11.1.2-1' }, '11.1.2-1', undefined)).toBe(true);
			expect(installedSatisfiesPin({ version: '11.1.2' }, '11.1.2', undefined)).toBe(true);
		});

		it('is false when a revision suffix differs from the installed bare version', () => {
			// The reported bug: installed 11.1.2, pinned 11.1.2-1 — these are NOT
			// the same, so the pin is not satisfied and an update must proceed.
			expect(installedSatisfiesPin({ version: '11.1.2' }, '11.1.2-1', undefined)).toBe(false);
			expect(installedSatisfiesPin({ version: '11.1.2-1' }, '11.1.2-2', undefined)).toBe(false);
		});

		it('is false when only the build metadata differs (compareBuild, not eq)', () => {
			// semver.eq ignores everything after `+`, so an exact pin like
			// 11.1.2+rev2 must not be considered already-satisfied by 11.1.2+rev1.
			expect(installedSatisfiesPin({ version: '11.1.2+rev1' }, '11.1.2+rev2', undefined)).toBe(
				false
			);
			expect(installedSatisfiesPin({ version: '11.1.2+rev1' }, '11.1.2+rev1', undefined)).toBe(
				true
			);
		});

		it('is false for "latest" or an unset desired version', () => {
			expect(installedSatisfiesPin({ version: '11.1.2' }, 'latest', undefined)).toBe(false);
			expect(installedSatisfiesPin({ version: '11.1.2' }, undefined, undefined)).toBe(false);
		});

		it('respects the runtime', () => {
			// no recorded runtime → compatible with anything
			expect(installedSatisfiesPin({ version: '11.1.2-1' }, '11.1.2-1', '-glibc')).toBe(true);
			// matching runtime
			expect(
				installedSatisfiesPin({ version: '11.1.2-1', runtime: '-glibc' }, '11.1.2-1', '-glibc')
			).toBe(true);
			// mismatched runtime → must re-download
			expect(
				installedSatisfiesPin({ version: '11.1.2-1', runtime: '-glibc' }, '11.1.2-1', '-musl')
			).toBe(false);
		});

		it('is false when nothing is installed', () => {
			expect(installedSatisfiesPin(undefined, '11.1.2-1', undefined)).toBe(false);
			expect(installedSatisfiesPin({ version: undefined }, '11.1.2-1', undefined)).toBe(false);
		});

		it('is false and does not throw on invalid semver input', () => {
			expect(installedSatisfiesPin({ version: 'not-a-version' }, '11.1.2-1', undefined)).toBe(
				false
			);
			expect(installedSatisfiesPin({ version: '11.1.2-1' }, 'not-a-version', undefined)).toBe(
				false
			);
		});
	});

	describe('prebuildIsRedundant', () => {
		it('never blocks an exact pin, even a revision suffix semver ranks lower', () => {
			// The reported bug lived here: 11.1.2-1 <= 11.1.2 is true in semver, but
			// because 11.1.2-1 is an exact pin it must still be installed.
			expect(prebuildIsRedundant({ version: '11.1.2' }, '11.1.2-1', '11.1.2-1', undefined)).toBe(
				false
			);
		});

		it('blocks a redundant/older resolve when using "latest"', () => {
			// same version already installed
			expect(prebuildIsRedundant({ version: '11.1.2' }, 'latest', '11.1.2', undefined)).toBe(true);
			// resolved latest is older
			expect(prebuildIsRedundant({ version: '11.1.3' }, 'latest', '11.1.2', undefined)).toBe(true);
			// unset desired behaves like latest
			expect(prebuildIsRedundant({ version: '11.1.2' }, undefined, '11.1.2', undefined)).toBe(true);
		});

		it('allows a newer "latest" resolve to download', () => {
			expect(prebuildIsRedundant({ version: '11.1.2' }, 'latest', '11.1.3', undefined)).toBe(false);
		});

		it('does not block when nothing is installed or the runtime differs', () => {
			expect(prebuildIsRedundant(undefined, 'latest', '11.1.2', undefined)).toBe(false);
			expect(
				prebuildIsRedundant({ version: '11.1.2', runtime: '-glibc' }, 'latest', '11.1.2', '-musl')
			).toBe(false);
		});

		it('does not block and does not throw on invalid semver input', () => {
			expect(prebuildIsRedundant({ version: 'not-a-version' }, 'latest', '11.1.2', undefined)).toBe(
				false
			);
			expect(prebuildIsRedundant({ version: '11.1.2' }, 'latest', 'not-a-version', undefined)).toBe(
				false
			);
		});
	});

	describe('the reported bug end to end', () => {
		it('pinning 11.1.2-1 over an installed 11.1.2 triggers a download', () => {
			const installed = { version: '11.1.2' };
			const desired = '11.1.2-1';
			const prebuildVersion = '11.1.2-1';

			// Neither guard short-circuits, so main.ts proceeds to downloadRocksDB.
			expect(installedSatisfiesPin(installed, desired, undefined)).toBe(false);
			expect(prebuildIsRedundant(installed, desired, prebuildVersion, undefined)).toBe(false);
		});
	});
});
