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
 * The packument probe must therefore send no cache directives: asking a CDN to revalidate would
 * show this process a version that installers, who ask for no such thing, still cannot see — the
 * gate would open during the exact window it exists to close.
 *
 * Observing one edge is not observing all of them. The settle therefore runs from the first
 * correct packument read, not from origin visibility: that read proves the CDN origin had the new
 * packument, so any edge still serving the old one fetched it earlier and expires within a TTL.
 * Anchoring on origin visibility instead would be unsound — this edge's own packument can lag past
 * a TTL, and an edge that refreshed during that lag would still be stale after the gate opened.
 */

/** `cache-control: max-age` npm serves the packument with. */
export const PACKUMENT_TTL_MS: number = 5 * 60 * 1000;

export const DEFAULT_REGISTRY: string = 'https://registry.npmjs.org';
export const DEFAULT_TIMEOUT_MS: number = 4 * PACKUMENT_TTL_MS;
export const DEFAULT_POLL_INTERVAL_MS: number = 10_000;

/** Caps one stalled request so it retries instead of consuming the whole package budget. */
export const DEFAULT_REQUEST_TIMEOUT_MS: number = 30_000;

export type VisibilityOptions = {
	registry?: string;
	/** Budget per package, not shared across them. */
	timeoutMs?: number;
	/** Quiet period after the first correct packument read, covering edges this process cannot see. */
	settleMs?: number;
	pollIntervalMs?: number;
	requestTimeoutMs?: number;
	fetch?: typeof globalThis.fetch;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	log?: (message: string) => void;
};

type ResolvedOptions = Required<VisibilityOptions>;

const trimTrailingSlash = (registry: string): string => registry.replace(/\/+$/, '');

const describeError = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

const defaultSleep = (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms));

/** Monotonic: a forward wall-clock correction must not satisfy the settle it never waited out. */
const defaultNow = (): number => performance.now();

const describeStall = (atOrigin: boolean, packumentSeen: boolean): string => {
	if (!atOrigin) {
		return 'not published at the origin';
	}
	return packumentSeen
		? 'packument current, still settling for other CDN edges'
		: 'published at the origin, packument still stale';
};

/**
 * `Infinity` and a negative value both parse as numbers and both defeat the budget — one never
 * expires, the other expires before the first poll — so only a finite positive duration is taken.
 */
export function parseTimeoutMs(value: string | undefined): number | undefined {
	if (value === undefined || value.trim() === '') {
		return undefined;
	}
	const parsed = Number(value);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

/** Call before doing any publishing: a budget below the settle can never open the gate. */
export function assertBudgetUsable(options: VisibilityOptions = {}): void {
	resolveOptions(options);
}

function resolveOptions(options: VisibilityOptions = {}): ResolvedOptions {
	const resolved = {
		registry: trimTrailingSlash(options.registry ?? DEFAULT_REGISTRY),
		timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
		settleMs: options.settleMs ?? PACKUMENT_TTL_MS,
		pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
		requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
		fetch: options.fetch ?? globalThis.fetch,
		now: options.now ?? defaultNow,
		sleep: options.sleep ?? defaultSleep,
		log: options.log ?? ((message: string) => console.log(message)),
	};

	if (resolved.settleMs >= resolved.timeoutMs) {
		throw new Error(
			`settleMs (${resolved.settleMs}ms) must be below timeoutMs (${resolved.timeoutMs}ms), ` +
				'otherwise the gate can never open'
		);
	}
	return resolved;
}

export function packumentUrl(registry: string, packageName: string): string {
	return `${trimTrailingSlash(registry)}/${encodeURIComponent(packageName)}`;
}

export function versionUrl(registry: string, packageName: string, version: string): string {
	return `${packumentUrl(registry, packageName)}/${version}`;
}

export async function visibleAtOrigin(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<boolean> {
	const { registry, fetch, requestTimeoutMs } = resolveOptions(options);
	// GET, not HEAD: npm documents this endpoint as GET and has had HEAD/GET discrepancies, and a
	// custom registry need not implement HEAD at all. Only the status is wanted, so the body is
	// cancelled rather than read — an unread body holds its socket out of the connection pool.
	const response = await fetch(versionUrl(registry, packageName, version), {
		signal: AbortSignal.timeout(requestTimeoutMs),
	});
	await response.body?.cancel();
	return response.status === 200;
}

/**
 * Deliberately sends no cache directives, so the answer is the one an installer gets from this
 * edge rather than a fresher one only this probe can see.
 */
export async function visibleToInstallers(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<boolean> {
	const { registry, fetch, requestTimeoutMs } = resolveOptions(options);
	const response = await fetch(packumentUrl(registry, packageName), {
		headers: { accept: 'application/vnd.npm.install-v1+json' },
		signal: AbortSignal.timeout(requestTimeoutMs),
	});
	if (response.status !== 200) {
		await response.body?.cancel();
		return false;
	}
	const packument = (await response.json()) as { versions?: Record<string, unknown> };
	return Boolean(packument.versions?.[version]);
}

export async function waitUntilServed(
	packageName: string,
	version: string,
	options: VisibilityOptions = {}
): Promise<void> {
	const resolved = resolveOptions(options);
	const { timeoutMs, settleMs, pollIntervalMs, now, sleep, log } = resolved;
	const spec = `${packageName}@${version}`;
	const deadline = now() + timeoutMs;
	let atOrigin = false;
	let packumentSeenAt: number | undefined;

	while (true) {
		try {
			if (!atOrigin && (await visibleAtOrigin(packageName, version, resolved))) {
				atOrigin = true;
				log(`origin is serving ${spec}; waiting for the packument to catch up`);
			}
			if (
				atOrigin &&
				packumentSeenAt === undefined &&
				(await visibleToInstallers(packageName, version, resolved))
			) {
				packumentSeenAt = now();
				log(`packument is serving ${spec}; settling for other CDN edges`);
			}
			if (packumentSeenAt !== undefined && now() >= packumentSeenAt + settleMs) {
				log(`npm is serving ${spec}`);
				return;
			}
		} catch (error) {
			// One failed or aborted request is not evidence the publish failed; only the deadline is.
			log(`probe for ${spec} failed (${describeError(error)}), retrying`);
		}

		if (now() >= deadline) {
			throw new Error(
				`Timed out after ${Math.round(timeoutMs / 1000)}s waiting for npm to serve ${spec} ` +
					`(${describeStall(atOrigin, packumentSeenAt !== undefined)})`
			);
		}
		log(`waiting for npm to serve ${spec}...`);
		await sleep(pollIntervalMs);
	}
}

export async function waitUntilAllServed(
	specs: Array<{ packageName: string; version: string }>,
	options: VisibilityOptions = {}
): Promise<void> {
	const results = await Promise.allSettled(
		specs.map(({ packageName, version }) => waitUntilServed(packageName, version, options))
	);
	const failures = results.filter((result) => result.status === 'rejected');

	if (failures.length > 0) {
		throw new Error(
			`${failures.length} of ${specs.length} package(s) are not being served yet:\n` +
				failures.map((failure) => `  - ${describeError(failure.reason)}`).join('\n'),
			{ cause: failures[0].reason }
		);
	}
}
