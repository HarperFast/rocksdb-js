import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import semver from 'semver';

type GithubRelease = {
	assets: {
		name: string;
		browser_download_url: string;
	}[];
	body: string;
	name: string;
	tag_name: string;
};

export type Prebuild = {
	version: string;
	assets: {
		name: string;
		url: string;
	}[];
};

export async function getPrebuild() {
	// download the latest RocksDB prebuild
	const headers: Record<string, string> = {
		Accept: 'application/vnd.github.v3.raw',
	};
	if (process.env.GITHUB_TOKEN) {
		headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
	}

	// get the latest RocksDB release
	const response = await fetch('https://api.github.com/repos/harperdb/rocksdb-prebuilds/releases', {
		headers,
	});

	const releases = await response.json() as GithubRelease[];
	releases.sort((a, b) => {
		const aVersion = a.tag_name.replace(/^v/, '');
		const bVersion = b.tag_name.replace(/^v/, '');
		return semver.rcompare(aVersion, bVersion);
	});

	// default to the latest release
	let prebuild: GithubRelease | undefined = releases[0];
	const pkgJson = JSON.parse(readFileSync(resolve(import.meta.dirname, '../../package.json'), 'utf8'));
	const version = process.env.ROCKSDB_VERSION || pkgJson.rocksdb?.version || 'latest';
	if (version && version !== 'latest') {
		prebuild = releases.find(release => release.tag_name === `v${version}`);
	}

	if (!prebuild) {
		throw new Error(`Prebuilt RocksDB ${version ? `v${version}` : 'releases'} not found!`);
	}

	return {
		assets: prebuild.assets.map((asset) => ({
			name: asset.name,
			url: asset.browser_download_url,
		})),
		version: prebuild.tag_name.replace(/^v/, ''),
	};
}
