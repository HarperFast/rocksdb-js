/**
 * Bootstrap-script builder for worker threads. Kept free of any `src` import so
 * fork fixtures can import it under Node's native type stripping without pulling
 * in the (bundler-resolved) source graph.
 */

/**
 * Creates a bootstrap script to run in a worker thread.
 *
 * @returns The script to run in a worker thread.
 */
export function createWorkerBootstrapScript(path: string): string {
	// Normalize Windows backslashes to forward slashes (valid in import specifiers
	// on every runtime) and escape single quotes before interpolating into the script.
	const normalizedPath = path.replace(/\\/g, '/').replace(/'/g, "\\'");
	if (process.versions.deno || process.versions.bun) {
		// Deno runs scripts as non-module, so we need to use dynamic import()
		return `import('node:url').then(({ pathToFileURL }) => import(pathToFileURL('${normalizedPath}')));`;
	}

	// Node natively strips the .mts worker's types and its src imports (which carry
	// real .ts extensions), so no build step is needed to run worker/fixture tests.
	return `import('${normalizedPath}');`;
}
