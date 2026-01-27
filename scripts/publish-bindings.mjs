/**
 * rocksdb-js platform specific bindings publish script.
 *
 * This script will publish all bindings in the `artifacts` directory as
 * separate target-specific packages to npm.
 *
 * Required environment variables:
 * - NODE_AUTH_TOKEN: The npm token to use for authentication.
 * - TAG: The tag to use for the packages: `latest` or `next`.
 *
 * @example
 * NODE_AUTH_TOKEN=... TAG=latest node scripts/publish-bindings.mjs
 */

import { execFileSync } from 'node:child_process';
import {
	copyFileSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

if (!process.env.NODE_AUTH_TOKEN) {
	throw new Error('NODE_AUTH_TOKEN environment variable is not set');
}

const __dirname = fileURLToPath(dirname(import.meta.url));
const packageJson = JSON.parse(readFileSync(resolve(__dirname, '..', 'package.json'), 'utf8'));
const tag = process.env.TAG || 'latest';
const artifactsDir = resolve(__dirname, '..', 'artifacts');
const name = 'rocksdb-js';
const bindingFilename = `${name}.node`;
const bindings = {};

// find all bindings in the artifacts directory
for (const target of readdirSync(artifactsDir)) {
	try {
		const binding = join(artifactsDir, target, bindingFilename);
		if (statSync(binding).isFile()) {
			console.log('Found binding:', binding);
			bindings[target] = binding;
		}
	} catch {
		// ignore
	}
}
console.log();

// cross check the bindings against the optionalDependencies
for (const dep of Object.keys(packageJson.optionalDependencies)) {
	const target = dep.replace(`${packageJson.name}-`, '');
	if (!bindings[target]) {
		throw new Error(`Binding for ${dep} not found in artifacts`);
	}
	console.log(`Matched binding for ${dep} -> ${relative(dirname(artifactsDir), bindings[target])}`);
}
console.log();

for (const target of Object.keys(bindings)) {
	const [platform, arch, libc] = target.split('-');
	const packageName = `${packageJson.name}-${target}`;
	const pkgInfo = {
		name: packageName,
		version: packageJson.version,
		description: `${target} binding for ${name}`,
		license: packageJson.license,
		homepage: packageJson.homepage,
		bugs: packageJson.bugs,
		repository: packageJson.repository,
		main: `./${bindingFilename}`,
		exports: { '.': `./${bindingFilename}` },
		files: [bindingFilename],
		preferUnplugged: true,
		engines: packageJson.engines,
		os: [platform],
		cpu: [arch],
		libc: libc ? [libc] : undefined,
	};
	const pkgJson = JSON.stringify(pkgInfo, null, 2);

	console.log('Publishing:', pkgJson);

	const tmpDir = join(tmpdir(), `${name}-${target}-${packageJson.version}`);
	mkdirSync(tmpDir, { recursive: true });

	copyFileSync(bindings[target], join(tmpDir, bindingFilename));
	writeFileSync(
		join(tmpDir, 'README.md'),
		`# ${name}-${target}\n\n`
			+ `${target} binding for [${name}](https://npmjs.com/package/${packageJson.name}).`
	);
	writeFileSync(join(tmpDir, 'package.json'), pkgJson);
	writeFileSync(
		join(tmpDir, '.npmrc'),
		`//registry.npmjs.org/:_authToken=${process.env.NODE_AUTH_TOKEN}\n`
	);

	try {
		execFileSync('pnpm', ['publish', '--access', 'public', '--tag', tag], {
			cwd: tmpDir,
			stdio: 'inherit',
			env: { ...process.env, FORCE_COLOR: '1' },
		});
	} catch (error) {
		console.error(`Failed to publish ${packageName}:`, error);
		process.exit(1);
	}

	console.log(`Published ${packageName} to npm\n`);
}
