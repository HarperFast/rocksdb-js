/**
 * Downloads and extracts GoogleTest for native unit tests.
 * Invoked by binding.gyp prepare-gtest before building rocksdb-js-native-tests.
 */

import { createHash } from 'node:crypto';
import { createWriteStream, existsSync } from 'node:fs';
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { pipeline } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { x as extractTar } from 'tar';

const GTEST_VERSION = '1.17.0';
const GTEST_SHA256 = '65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c';

const __dirname = dirname(fileURLToPath(import.meta.url));
const streamPipeline = promisify(pipeline);

async function main() {
	const dest = resolve(__dirname, '../../deps/googletest');
	const marker = join(dest, 'googletest/include/gtest/gtest.h');

	if (existsSync(marker)) {
		console.log(`GoogleTest ${GTEST_VERSION} is already installed.`);
		process.exit(0);
	}

	const url = `https://github.com/google/googletest/archive/refs/tags/v${GTEST_VERSION}.tar.gz`;
	const tmpFile = join(tmpdir(), `googletest-${GTEST_VERSION}.tar.gz`);
	const extractDir = join(tmpdir(), `googletest-extract-${GTEST_VERSION}`);

	console.log(`Downloading GoogleTest ${GTEST_VERSION}...`);
	const response = await fetch(url);
	if (!response.ok || !response.body) {
		throw new Error(`Failed to download ${url} (${response.status} ${response.statusText})`);
	}

	await mkdir(dirname(tmpFile), { recursive: true });
	const fileStream = createWriteStream(tmpFile);
	await streamPipeline(response.body, fileStream);

	const sha256 = createHash('sha256')
		.update(await readFile(tmpFile))
		.digest('hex');
	if (sha256 !== GTEST_SHA256) {
		throw new Error(`SHA256 mismatch for ${tmpFile} (${sha256} !== ${GTEST_SHA256})`);
	}

	await rm(extractDir, { recursive: true, force: true });
	await rm(dest, { recursive: true, force: true });
	await mkdir(extractDir, { recursive: true });

	console.log(`Extracting ${tmpFile}...`);
	await extractTar({ file: tmpFile, cwd: extractDir });

	const extracted = join(extractDir, `googletest-${GTEST_VERSION}`);
	await mkdir(dirname(dest), { recursive: true });
	await rename(extracted, dest);

	await writeFile(join(dest, '.version'), `${GTEST_VERSION}\n`, 'utf8');
	console.log(`GoogleTest ${GTEST_VERSION} installed to ${dest}`);
}

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
