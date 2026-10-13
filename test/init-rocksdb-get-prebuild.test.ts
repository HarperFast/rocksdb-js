import { getPrebuild } from '../scripts/init-rocksdb/get-prebuild.ts';
import { afterEach, describe, expect, it, vi } from 'vitest';

function release(tag: string, draft = false) {
	return {
		assets: [
			{
				name: `rocksdb-${tag}-linux-x64.tar.xz`,
				browser_download_url: `https://example.test/${tag}`,
			},
		],
		body: '',
		draft,
		name: `RocksDB ${tag}`,
		tag_name: tag,
	};
}

function stubReleases(releases: ReturnType<typeof release>[]) {
	vi.stubGlobal(
		'fetch',
		vi.fn(async () => new Response(JSON.stringify(releases), { status: 200 }))
	);
}

describe('init-rocksdb getPrebuild', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('resolves latest to the newest published release, skipping a newer draft', async () => {
		stubReleases([release('v11.9.0', true), release('v11.8.1'), release('v11.7.0')]);
		const prebuild = await getPrebuild();
		expect(prebuild.version).toBe('11.8.1');
		expect(prebuild.assets[0].url).toBe('https://example.test/v11.8.1');
	});

	it('does not resolve a pinned version that exists only as a draft', async () => {
		stubReleases([release('v11.9.0', true), release('v11.8.1')]);
		await expect(getPrebuild('11.9.0')).rejects.toThrow('Prebuilt RocksDB v11.9.0 not found!');
	});

	it('resolves a pinned published version', async () => {
		stubReleases([release('v11.9.0', true), release('v11.8.1')]);
		expect((await getPrebuild('11.8.1')).version).toBe('11.8.1');
	});
});
