/**
 * Registry visibility gate for the platform-specific binding packages.
 *
 * A successful `pnpm publish` means npm accepted the tarball, not that the version resolves. The
 * parent package's `optionalDependencies` pin all eight bindings, so an install landing between
 * the two resolves the missing one to nothing — silently, because a 404 on an optional dependency
 * is not an error — and writes a lockfile that no later `npm ci` can install. The parent must
 * never become resolvable ahead of its own bindings.
 *
 * Two endpoints answer "is this published", with different caching contracts, and the gate needs
 * both:
 *
 * - `/<pkg>/<version>` is uncached (`cf-cache-status: DYNAMIC`, no `cache-control`) and reports
 *   the origin's state immediately.
 * - `/<pkg>` is the packument, served from the CDN with `cache-control: public, max-age=300`. It
 *   lags the origin by up to that TTL and is what an installer actually resolves
 *   `optionalDependencies` through, so origin agreement alone does not make a version installable.
 *
 * Only one CDN edge is observable from here, so a consumer on another edge can still trail by up
 * to one TTL after this gate opens.
 */

/** `cache-control: max-age` npm serves the packument with; a visibility budget needs several. */
export const PACKUMENT_TTL_MS: number = 5 * 60 * 1000;

export const DEFAULT_REGISTRY: string = 'https://registry.npmjs.org';
export const DEFAULT_TIMEOUT_MS: number = 3 * PACKUMENT_TTL_MS;
export const DEFAULT_POLL_INTERVAL_MS: number = 10_000;

export type VisibilityOptions = {
	registry?: string;
	/** Budget per package, not shared across them. */
	timeoutMs?: number;
	pollIntervalMs?: number;
	fetch?: typeof globalThis.fetch;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	log?: (message: string) => void;
};

type ResolvedOptions = Required<VisibilityOptions>;

const defaultSleep = (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms));

function resolveOptions(options: VisibilityOptions = {}): ResolvedOptions {
	return {
		registry: (options.registry ?? DEFAULT_REGISTRY).replace(/\/+$/, ''),
		timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
		pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
		fetch: options.fetch ?? globalThis.fetch,
		now: options.now ?? Date.now,
		sleep: options.sleep ?? defaultSleep,
		log: options.log ?? ((message: string) => console.log(message)),
	};
}

export function versionUrl(registry: string, packageName: string, version: string): string {
	return `${registry.replace(/\/+$/, '')}/${encodeURIComponent(packageName)}/${version}`;
}

export function packumentUrl(registry: string, packageName: string): string {
	return `${registry.replace(/\/+$/, '')}/${encodeURIComponent(packageName)}`;
}

/** Origin truth, via the endpoint npm does not put behind the CDN. */
export async function visibleAtOrigin(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<boolean> {
	const { registry, fetch } = resolveOptions(options);
	const response = await fetch(versionUrl(registry, packageName, version), {
		headers: { 'cache-control': 'no-cache' },
	});
	return response.status === 200;
}

/** What an installer resolves through: the CDN-cached abbreviated packument. */
export async function visibleToInstallers(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<boolean> {
	const { registry, fetch } = resolveOptions(options);
	const response = await fetch(packumentUrl(registry, packageName), {
		headers: {
			accept: 'application/vnd.npm.install-v1+json',
			'cache-control': 'no-cache',
		},
	});
	if (response.status !== 200) {
		return false;
	}
	const packument = (await response.json()) as { versions?: Record<string, unknown> };
	return Boolean(packument.versions?.[version]);
}

/**
 * Block until `packageName@version` is both published at the origin and resolvable through the
 * packument, or the per-package budget expires.
 */
export async function waitUntilServed(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<void> {
	const resolved = resolveOptions(options);
	const { timeoutMs, pollIntervalMs, now, sleep, log } = resolved;
	const spec = `${packageName}@${version}`;
	const deadline = now() + timeoutMs;
	let atOrigin = false;

	while (true) {
		try {
			if (!atOrigin && (await visibleAtOrigin(packageName, version, resolved))) {
				atOrigin = true;
				log(`origin is serving ${spec}; waiting for the packument to catch up`);
			}
			if (atOrigin && (await visibleToInstallers(packageName, version, resolved))) {
				log(`npm is serving ${spec}`);
				return;
			}
		} catch (error) {
			// One failed request is not evidence the publish failed; only the deadline is.
			log(`probe for ${spec} failed (${(error as Error).message}), retrying`);
		}

		if (now() >= deadline) {
			throw new Error(
				`Timed out after ${Math.round(timeoutMs / 1000)}s waiting for npm to serve ${spec} ` +
					`(${atOrigin ? 'published at the origin, packument still stale' : 'not published at the origin'})`
			);
		}
		log(`waiting for npm to serve ${spec}...`);
		await sleep(pollIntervalMs);
	}
}

/**
 * Wait for every spec concurrently, each on its own budget, and report all failures rather than
 * only the first.
 */
export async function waitUntilAllServed(
	specs: Array<{ packageName: string; version: string }>,
	options: VisibilityOptions = {}
): Promise<void> {
	const results = await Promise.allSettled(
		specs.map(({ packageName, version }) => waitUntilServed(packageName, version, options))
	);
	const reasons = results
		.filter((result) => result.status === 'rejected')
		.map((result) => (result.reason as Error).message);

	if (reasons.length > 0) {
		throw new Error(
			`${reasons.length} of ${specs.length} package(s) are not being served yet:\n` +
				reasons.map((reason) => `  - ${reason}`).join('\n')
		);
	}
}
