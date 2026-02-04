import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import semver from 'semver';

if (process.argv.length < 3) {
	console.error('Usage: node scripts/bump-versions.mjs <new-version>');
	process.exit(1);
}

const __dirname = fileURLToPath(dirname(import.meta.url));
const packageJson = JSON.parse(readFileSync(resolve(__dirname, '..', 'package.json'), 'utf8'));
const currentVersion = packageJson.version;
const newVersion = process.argv[2];

if (!semver.valid(newVersion)) {
	console.error(`ERROR: Invalid version "${newVersion}"`);
	process.exit(1);
}

if (semver.lte(newVersion, currentVersion)) {
	console.error(`ERROR: New version ${newVersion} is less than current version ${currentVersion}`);
	process.exit(1);
}

packageJson.version = newVersion;
for (const key of Object.keys(packageJson.optionalDependencies)) {
	if (key.startsWith(packageJson.name)) {
		packageJson.optionalDependencies[key] = newVersion;
	}
}

writeFileSync(resolve(__dirname, '..', 'package.json'), JSON.stringify(packageJson, null, 2));

console.log(`🎉 Bumped version from ${currentVersion} to ${newVersion}`);
