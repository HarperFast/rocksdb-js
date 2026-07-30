import semver from 'semver';

/** The version/runtime of the RocksDB prebuild currently installed in `deps/rocksdb`. */
export type InstalledVersion = { version?: string; runtime?: string };

/**
 * Whether `desiredVersion` is an exact version pin (a concrete version from
 * `package.json`'s `rocksdb.version` or the `ROCKSDB_VERSION` env var) rather
 * than `latest` or unset.
 *
 * The distinction matters because a downstream revision suffix such as
 * `11.1.2-1` is a *prerelease* to semver, which sorts it **below** the bare
 * `11.1.2`. So a pinned `11.1.2-1` must be installed exactly as requested — the
 * "don't downgrade" guard used for `latest` would otherwise refuse it, thinking
 * the installed `11.1.2` is newer.
 */
export function isExactVersionPin(desiredVersion: string | undefined): desiredVersion is string {
	return !!desiredVersion && desiredVersion !== 'latest';
}

/**
 * Whether the installed prebuild already satisfies an exact version pin, so no
 * download is needed. Requires an exact pin, a matching version (prerelease
 * suffix included), and a compatible runtime. Never true for `latest`/unset —
 * that case is handled by {@link prebuildIsRedundant} after the latest release
 * is resolved.
 */
export function installedSatisfiesPin(
	installed: InstalledVersion | undefined,
	desiredVersion: string | undefined,
	runtime: string | undefined
): boolean {
	return (
		isExactVersionPin(desiredVersion) &&
		!!installed?.version &&
		// Guard against a corrupted rocksdb.json or a non-semver ROCKSDB_VERSION:
		// the semver comparators throw on invalid input. Treating them as "not
		// satisfied" falls through to getPrebuild, which fails with a clear "not
		// found". Use compareBuild (not eq) so build metadata is part of the
		// identity — an exact pin like 11.1.2+rev2 must not be satisfied by an
		// installed 11.1.2+rev1 (eq ignores everything after `+`).
		!!semver.valid(installed.version) &&
		!!semver.valid(desiredVersion) &&
		semver.compareBuild(installed.version, desiredVersion) === 0 &&
		runtimeMatches(installed, runtime)
	);
}

/**
 * Whether a resolved `latest` prebuild is not newer than what's installed, so
 * downloading it would be a redundant re-install or a downgrade.
 *
 * Only applies when the version is **not** an exact pin: an explicit pin must
 * always install exactly what was requested, even a revision suffix (e.g.
 * `11.1.2` → `11.1.2-1`) that semver ranks lower than the bare release.
 */
export function prebuildIsRedundant(
	installed: InstalledVersion | undefined,
	desiredVersion: string | undefined,
	prebuildVersion: string,
	runtime: string | undefined
): boolean {
	return (
		!isExactVersionPin(desiredVersion) &&
		!!installed?.version &&
		// See installedSatisfiesPin: guard invalid input so a corrupted version
		// never throws. "Not redundant" falls through to a (re)download.
		!!semver.valid(installed.version) &&
		!!semver.valid(prebuildVersion) &&
		semver.lte(prebuildVersion, installed.version) &&
		runtimeMatches(installed, runtime)
	);
}

/**
 * A runtime is compatible when the installed prebuild has no recorded runtime
 * (older prebuilds and source builds) or it matches the target runtime.
 */
function runtimeMatches(installed: InstalledVersion, runtime: string | undefined): boolean {
	return !installed.runtime || installed.runtime === runtime;
}
