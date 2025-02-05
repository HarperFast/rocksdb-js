import { execFileSync, execSync } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pipeline } from 'node:stream';
import { promisify } from 'node:util';
import type { Prebuild } from './get-prebuild';

const platformMap = {
	darwin: 'osx',
	win32: 'windows',
};

const streamPipeline = promisify(pipeline);

export async function downloadRocksDB(prebuild: Prebuild, dest: string) {
	const filename = `rocksdb-${prebuild.version}-${process.arch}-${platformMap[process.platform] || process.platform}`;
	const [asset] = prebuild.assets.filter((asset) => asset.name.startsWith(filename));
	if (!asset) {
		throw new Error('No asset found');
	}

	const { name, url } = asset;
	const tmpFile = join(tmpdir(), name);

	try {
		console.log(`Downloading ${url}`);
		const headers: Record<string, string> = {};
		if (process.env.GH_TOKEN) {
			headers.Authorization = `Bearer ${process.env.GH_TOKEN}`;
		}
		const response = await fetch(url, { headers });
		if (!response.ok || !response.body) {
			throw new Error(`Failed to download ${url} (${response.status} ${response.statusText})`);
		}

		// stream the response to the destination file
		const fileStream = createWriteStream(tmpFile);
		await streamPipeline(response.body, fileStream);

		await rm(dest, { recursive: true, force: true });
		await mkdir(dest, { recursive: true });

		// extract the file
		console.log(`Extracting ${tmpFile}`);
		if (process.platform === 'win32') {
			execSync(`7z x "${tmpFile}" -o"${dirname(tmpFile)}"`, { stdio: 'inherit' });
			execSync(`dir "${dirname(tmpFile)}"`, { stdio: 'inherit' });
			console.log(`Extracting ${tmpFile.replace(/\.xz$/, '')}`);
			execSync(`7z x "${tmpFile.replace(/\.xz$/, '')}" -o"${dest}"`, { stdio: 'inherit' });
		} else {
			execFileSync('tar', ['-xf', tmpFile, '-C', dest], { stdio: 'inherit' });
		}
	} finally {
		await rm(tmpFile, { force: true });
		await rm(tmpFile.replace(/\.xz$/, ''), { force: true });
	}
}
