/**
 * Resolves the effective RocksDB build selection for the native-test runner.
 *
 * Mirrors init-rocksdb's precedence (the caller loads `.env` with override first):
 * a `ROCKSDB_PATH` source build wins, else `ROCKSDB_VERSION`, else the package pin
 * (`pkg.rocksdb.version`); the Linux runtime suffix (glibc/musl) is appended.
 *
 * - `expectedVersion` feeds `ROCKSDB_EXPECTED_VERSION` (the native
 *   `RocksDBVersion.MatchesPackagePin` check).
 * - `identity` is the rebuild marker: it must change whenever the linked artifact
 *   would change — a different env override, runtime, or source — even if the
 *   package pin is unchanged, so the runner never reuses a stale binary. (This is
 *   why the pin alone was insufficient: a build under `ROCKSDB_VERSION=11.1.2-1`
 *   must not be recorded as the pin `11.1.2-2`.)
 */
export function resolveBuildSelection(env, pkg, platform) {
	const runtime = platform === 'linux' ? `-${env.ROCKSDB_LIBC || 'glibc'}` : '';
	const expectedVersion = env.ROCKSDB_VERSION || pkg.rocksdb?.version || '';
	const identity = env.ROCKSDB_PATH ? `source:${env.ROCKSDB_PATH}` : `${expectedVersion}${runtime}`;
	return { expectedVersion, identity, runtime };
}
