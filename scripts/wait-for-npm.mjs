/**
 * Block until npm serves a published package version.
 *
 * Optional environment variables:
 * - PUBLISH_VISIBILITY_TIMEOUT_MS: Budget for the wait (default 15 minutes).
 *
 * @example
 * node scripts/wait-for-npm.mjs @harperfast/rocksdb-js 2.11.0
 */

import { parseTimeoutMs, waitUntilAllServed } from './publish-bindings/npm-visibility.ts';

const [packageName, version] = process.argv.slice(2);

if (!packageName || !version) {
	console.error('Usage: node scripts/wait-for-npm.mjs <package-name> <version>');
	process.exit(1);
}

const timeoutMs = parseTimeoutMs(process.env.PUBLISH_VISIBILITY_TIMEOUT_MS);

try {
	await waitUntilAllServed([{ packageName, version }], {
		registry: process.env.NPM_CONFIG_REGISTRY,
		timeoutMs,
	});
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
