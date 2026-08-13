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
	if (process.versions.deno || process.versions.bun) {
		// Deno runs scripts as non-module, so we need to use dynamic import()
		return `import('node:url').then(({ pathToFileURL }) => import(pathToFileURL('${path.replace(/'/g, "\\'")}')));`;
	}

	// Node natively strips the .mts worker's types; it imports the built dist bundle.
	return `import('${path}');`;
}
