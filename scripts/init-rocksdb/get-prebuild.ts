import semver from 'semver';

type GithubRelease = {
	assets: { name: string; browser_download_url: string }[];
	body: string;
	name: string;
	tag_name: string;
};

export type Prebuild = { version: string; assets: { name: string; url: string }[] };

export async function getPrebuild(desiredVersion?: string): Promise<Prebuild> {
	// download the latest RocksDB prebuild
	const headers: Record<string, string> = { Accept: 'application/vnd.github.v3.raw' };
	if (process.env.GH_TOKEN) {
		headers.Authorization = `Bearer ${process.env.GH_TOKEN}`;
	}

	// get the latest RocksDB release
	const response = await fetch(
		'https://api.github.com/repos/harperfast/rocksdb-prebuilds/releases',
		{ headers }
	);

	if (!response.ok) {
		throw new Error(
			`Failed to fetch latest RocksDB release (${response.status} ${response.statusText})`
		);
	}

	const releases = (await response.json()) as GithubRelease[];
	releases.sort((a, b) => {
		const aVersion = a.tag_name.replace(/^v/, '');
		const bVersion = b.tag_name.replace(/^v/, '');
		return semver.rcompare(aVersion, bVersion);
	});

	// default to the latest release
	let prebuild: GithubRelease | undefined = releases[0];
	if (desiredVersion && desiredVersion !== 'latest') {
		prebuild = releases.find((release) => release.tag_name === `v${desiredVersion}`);
	}

	if (!prebuild) {
		throw new Error(
			`Prebuilt RocksDB ${desiredVersion ? `v${desiredVersion}` : 'releases'} not found!`
		);
	}

	return {
		assets: prebuild.assets.map((asset) => ({ name: asset.name, url: asset.browser_download_url })),
		version: prebuild.tag_name.replace(/^v/, ''),
	};
}
