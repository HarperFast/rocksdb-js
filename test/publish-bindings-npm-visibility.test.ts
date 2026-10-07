import {
	DEFAULT_POLL_INTERVAL_MS,
	DEFAULT_TIMEOUT_MS,
	PACKUMENT_TTL_MS,
	packumentUrl,
	parseTimeoutMs,
	versionUrl,
	visibleAtOrigin,
	visibleToInstallers,
	waitUntilAllServed,
	waitUntilServed,
} from '../scripts/publish-bindings/npm-visibility.ts';
import { describe, expect, it } from 'vitest';

const REGISTRY = 'https://registry.npmjs.org';

/**
 * A registry whose origin and packument become truthful at independently configured times, driven
 * by a virtual clock. The gap between them models the CDN `max-age=300` lag the gate absorbs.
 */
function fakeRegistry(
	published: Record<string, { originAtMs: number; packumentAtMs: number }>,
	clock: { now: () => number }
) {
	const requests: string[] = [];
	const methods: string[] = [];
	const cacheDirectives: Array<string | undefined> = [];
	let bodiesCancelled = 0;
	const bodyStub = () => ({
		cancel: async () => {
			bodiesCancelled += 1;
		},
	});

	const fetch = (async (
		url: string,
		init?: { headers?: Record<string, string>; method?: string }
	) => {
		requests.push(url);
		methods.push(init?.method ?? 'GET');
		cacheDirectives.push(init?.headers?.['cache-control']);

		const wantsPackument = init?.headers?.accept?.includes('install-v1') ?? false;
		const entry = Object.entries(published).find(([spec]) => {
			const [packageName, version] = splitSpec(spec);
			return (
				url ===
				(wantsPackument
					? packumentUrl(REGISTRY, packageName)
					: versionUrl(REGISTRY, packageName, version))
			);
		});

		if (!entry) {
			return { status: 404, body: bodyStub(), json: async () => ({}) };
		}
		const [spec, timing] = entry;
		const [, version] = splitSpec(spec);
		if (wantsPackument) {
			const visible = clock.now() >= timing.packumentAtMs;
			return {
				status: 200,
				body: bodyStub(),
				json: async () => ({ versions: visible ? { [version]: {} } : {} }),
			};
		}
		return {
			status: clock.now() >= timing.originAtMs ? 200 : 404,
			body: bodyStub(),
			json: async () => ({}),
		};
	}) as unknown as typeof globalThis.fetch;

	return { fetch, requests, methods, cacheDirectives, cancelled: () => bodiesCancelled };
}

function splitSpec(spec: string): [string, string] {
	const at = spec.lastIndexOf('@');
	return [spec.slice(0, at), spec.slice(at + 1)];
}

/**
 * Advances only when the code under test sleeps, so a 15 minute budget costs no real time.
 * Concurrent sleepers each advance it, so elapsed time runs fast in proportion to the number of
 * waiters; assertions here compare clock values, never poll counts across concurrent waits.
 */
function virtualClock() {
	let current = 0;
	return {
		now: () => current,
		sleep: async (ms: number) => {
			current += ms;
		},
		advance: (ms: number) => {
			current += ms;
		},
	};
}

/** Short budgets need the settle disabled, since a settle at or above the budget cannot open. */
function shortBudget(clock: ReturnType<typeof virtualClock>) {
	return {
		now: clock.now,
		sleep: clock.sleep,
		timeoutMs: 60_000,
		settleMs: 0,
		log: () => {},
	};
}

describe('publish-bindings npm-visibility', () => {
	describe('url building', () => {
		it('encodes a scoped package name', () => {
			expect(versionUrl(REGISTRY, '@harperfast/rocksdb-js-darwin-arm64', '2.11.0')).toBe(
				'https://registry.npmjs.org/%40harperfast%2Frocksdb-js-darwin-arm64/2.11.0'
			);
			expect(packumentUrl(REGISTRY, '@harperfast/rocksdb-js-darwin-arm64')).toBe(
				'https://registry.npmjs.org/%40harperfast%2Frocksdb-js-darwin-arm64'
			);
		});

		it('tolerates a trailing slash on the registry', () => {
			expect(versionUrl('https://registry.npmjs.org/', 'pkg', '1.0.0')).toBe(
				'https://registry.npmjs.org/pkg/1.0.0'
			);
		});
	});

	describe('parseTimeoutMs', () => {
		it('takes a finite positive duration', () => {
			expect(parseTimeoutMs('30000')).toBe(30_000);
		});

		it('rejects Infinity and non-positive values', () => {
			expect(parseTimeoutMs('Infinity')).toBeUndefined();
			expect(parseTimeoutMs('-5000')).toBeUndefined();
			expect(parseTimeoutMs('0')).toBeUndefined();
		});

		it('rejects unset, empty and malformed values', () => {
			expect(parseTimeoutMs(undefined)).toBeUndefined();
			expect(parseTimeoutMs('   ')).toBeUndefined();
			expect(parseTimeoutMs('soon')).toBeUndefined();
		});
	});

	describe('probes', () => {
		it('reads origin state with GET and releases the body', async () => {
			const clock = virtualClock();
			const { fetch, requests, methods, cancelled } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: 0 } },
				clock
			);
			expect(await visibleAtOrigin('pkg', '1.0.0', { fetch })).toBe(true);
			expect(requests).toEqual(['https://registry.npmjs.org/pkg/1.0.0']);
			expect(methods).toEqual(['GET']);
			expect(cancelled()).toBe(1);
		});

		it('asks the packument for no cache revalidation', async () => {
			const clock = virtualClock();
			const { fetch, cacheDirectives } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: 0 } },
				clock
			);
			await visibleToInstallers('pkg', '1.0.0', { fetch });
			expect(cacheDirectives).toEqual([undefined]);
		});

		it('reports a version absent from the packument as not installable', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: 1000 } },
				clock
			);
			expect(await visibleToInstallers('pkg', '1.0.0', { fetch })).toBe(false);
			clock.advance(1000);
			expect(await visibleToInstallers('pkg', '1.0.0', { fetch })).toBe(true);
		});

		it('treats an unknown package as not published', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry({}, clock);
			expect(await visibleAtOrigin('nope', '1.0.0', { fetch })).toBe(false);
			expect(await visibleToInstallers('nope', '1.0.0', { fetch })).toBe(false);
		});
	});

	describe('waitUntilServed', () => {
		it('does not open on origin agreement alone — the packument gates it', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: PACKUMENT_TTL_MS } },
				clock
			);
			await waitUntilServed('pkg', '1.0.0', {
				fetch,
				now: clock.now,
				sleep: clock.sleep,
				log: () => {},
			});
			expect(clock.now()).toBeGreaterThanOrEqual(2 * PACKUMENT_TTL_MS);
		});

		it('holds for a TTL after the packument is first correct, covering other edges', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry({ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: 0 } }, clock);
			await waitUntilServed('pkg', '1.0.0', {
				fetch,
				now: clock.now,
				sleep: clock.sleep,
				log: () => {},
			});
			expect(clock.now()).toBeGreaterThanOrEqual(PACKUMENT_TTL_MS);
		});

		// Anchoring on origin visibility would have opened the gate at the packument read; an edge
		// that refreshed during this edge's lag would still have been stale.
		it('starts the settle at the packument read, not at origin visibility', async () => {
			const clock = virtualClock();
			const packumentLag = 2 * PACKUMENT_TTL_MS;
			const { fetch } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: packumentLag } },
				clock
			);
			await waitUntilServed('pkg', '1.0.0', {
				fetch,
				now: clock.now,
				sleep: clock.sleep,
				log: () => {},
			});
			expect(clock.now()).toBeGreaterThanOrEqual(packumentLag + PACKUMENT_TTL_MS);
		});

		it('spends the settle across polls rather than satisfying it in one check', async () => {
			let current = 0;
			let polls = 0;
			const clockSpy = {
				now: () => current,
				sleep: async (ms: number) => {
					current += ms;
					polls += 1;
				},
			};
			const { fetch } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: 0 } },
				{ now: () => current }
			);
			await waitUntilServed('pkg', '1.0.0', { fetch, ...clockSpy, log: () => {} });
			expect(polls).toBeGreaterThanOrEqual(PACKUMENT_TTL_MS / DEFAULT_POLL_INTERVAL_MS);
		});

		it('refuses a settle at or above the budget rather than never opening', async () => {
			await expect(
				waitUntilServed('pkg', '1.0.0', { timeoutMs: 1000, settleMs: 1000 })
			).rejects.toThrow(/settleMs .* must be below timeoutMs/);
		});

		it('times out and names the gate it was waiting on', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry(
				{ 'pkg@1.0.0': { originAtMs: 0, packumentAtMs: Number.MAX_SAFE_INTEGER } },
				clock
			);
			await expect(
				waitUntilServed('pkg', '1.0.0', { fetch, ...shortBudget(clock) })
			).rejects.toThrow(/published at the origin, packument still stale/);
		});

		it('reports a package that never reached the origin differently', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry({}, clock);
			await expect(
				waitUntilServed('pkg', '1.0.0', { fetch, ...shortBudget(clock) })
			).rejects.toThrow(/not published at the origin/);
		});

		// The request never settles on its own, so this reaches a deadline only because each request
		// carries an abort signal. Remove the signals and it hangs rather than fails.
		it('bounds a request that never settles, by its signal alone', async () => {
			const clock = virtualClock();
			let calls = 0;
			const fetch = (async (_url: string, init?: { signal?: AbortSignal }) => {
				calls += 1;
				return new Promise((_resolve, reject) => {
					if (!init?.signal) {
						return; // no signal: never settles, and the test times out
					}
					init.signal.addEventListener('abort', () =>
						reject(Object.assign(new Error('This operation was aborted'), { name: 'TimeoutError' }))
					);
				});
			}) as unknown as typeof globalThis.fetch;

			await expect(
				waitUntilServed('pkg', '1.0.0', {
					fetch,
					...shortBudget(clock),
					requestTimeoutMs: 5,
				})
			).rejects.toThrow(/not published at the origin/);
			expect(calls).toBeGreaterThan(1);
		});

		it('recovers after a transient request failure', async () => {
			const clock = virtualClock();
			let calls = 0;
			const fetch = (async (_url: string, init?: { headers?: Record<string, string> }) => {
				calls += 1;
				if (calls === 1) {
					throw new Error('ECONNRESET');
				}
				const wantsPackument = init?.headers?.accept?.includes('install-v1') ?? false;
				return wantsPackument
					? { status: 200, body: null, json: async () => ({ versions: { '1.0.0': {} } }) }
					: { status: 200, body: null, json: async () => ({}) };
			}) as unknown as typeof globalThis.fetch;

			await expect(
				waitUntilServed('pkg', '1.0.0', { fetch, ...shortBudget(clock) })
			).resolves.toBeUndefined();
			expect(calls).toBeGreaterThan(1);
		});
	});

	describe('budget ownership', () => {
		it('gives each package a fresh budget rather than one shared across the release', async () => {
			const clock = virtualClock();
			// Leaves exactly room for the settle that follows the packument read.
			const nearlyTheWholeBudget =
				DEFAULT_TIMEOUT_MS - PACKUMENT_TTL_MS - 2 * DEFAULT_POLL_INTERVAL_MS;
			const { fetch } = fakeRegistry(
				{
					'first@1.0.0': { originAtMs: 0, packumentAtMs: nearlyTheWholeBudget },
					'second@1.0.0': { originAtMs: 0, packumentAtMs: 2 * nearlyTheWholeBudget },
				},
				clock
			);
			const options = { fetch, now: clock.now, sleep: clock.sleep, log: () => {} };

			await waitUntilServed('first', '1.0.0', options);
			expect(clock.now()).toBeGreaterThanOrEqual(nearlyTheWholeBudget);

			await waitUntilServed('second', '1.0.0', options);
			expect(clock.now()).toBeGreaterThan(DEFAULT_TIMEOUT_MS);
		});

		// Proves non-serialization only. Both waits start together, so one deadline anchored around
		// the concurrent map behaves identically to one per package; the sequential case above is
		// what pins the anchoring.
		it('serves slow and fast packages together through waitUntilAllServed', async () => {
			const clock = virtualClock();
			// Leaves exactly room for the settle that follows the packument read.
			const nearlyTheWholeBudget =
				DEFAULT_TIMEOUT_MS - PACKUMENT_TTL_MS - 2 * DEFAULT_POLL_INTERVAL_MS;
			const { fetch } = fakeRegistry(
				{
					'slow@1.0.0': { originAtMs: 0, packumentAtMs: nearlyTheWholeBudget },
					'quick@1.0.0': { originAtMs: 0, packumentAtMs: PACKUMENT_TTL_MS },
				},
				clock
			);

			await expect(
				waitUntilAllServed(
					[
						{ packageName: 'slow', version: '1.0.0' },
						{ packageName: 'quick', version: '1.0.0' },
					],
					{ fetch, now: clock.now, sleep: clock.sleep, log: () => {} }
				)
			).resolves.toBeUndefined();
			expect(clock.now()).toBeLessThan(2 * DEFAULT_TIMEOUT_MS);
		});
	});

	describe('waitUntilAllServed', () => {
		it('probes every package concurrently rather than one after another', async () => {
			const clock = virtualClock();
			const { fetch, requests } = fakeRegistry(
				{
					'a@1.0.0': { originAtMs: 0, packumentAtMs: PACKUMENT_TTL_MS },
					'b@1.0.0': { originAtMs: 0, packumentAtMs: PACKUMENT_TTL_MS },
					'c@1.0.0': { originAtMs: 0, packumentAtMs: PACKUMENT_TTL_MS },
				},
				clock
			);

			await waitUntilAllServed(
				['a', 'b', 'c'].map((packageName) => ({ packageName, version: '1.0.0' })),
				{ fetch, now: clock.now, sleep: clock.sleep, log: () => {} }
			);

			expect(new Set(requests.slice(0, 3)).size).toBe(3);
		});

		it('reports every package that failed, not just the first', async () => {
			const clock = virtualClock();
			const { fetch } = fakeRegistry({ 'ok@1.0.0': { originAtMs: 0, packumentAtMs: 0 } }, clock);
			await expect(
				waitUntilAllServed(
					[
						{ packageName: 'ok', version: '1.0.0' },
						{ packageName: 'missing-a', version: '1.0.0' },
						{ packageName: 'missing-b', version: '1.0.0' },
					],
					{ fetch, ...shortBudget(clock) }
				)
			).rejects.toThrow(/2 of 3 package\(s\)[\s\S]*missing-a[\s\S]*missing-b/);
		});
	});
});
