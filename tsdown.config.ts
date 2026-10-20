import replace from '@rollup/plugin-replace';
import { readFileSync } from 'node:fs';
import { defineConfig, type UserConfig } from 'tsdown';

const { version } = JSON.parse(readFileSync('./package.json', 'utf8'));

const config: UserConfig = defineConfig({
	// The CJS build is a published entry point (`main` / `require`), not a legacy leftover.
	checks: { legacyCjs: false },
	deps: {
		neverBundle: ['msgpackr', 'ordered-binary'],
	},
	entry: './src/index.ts',
	format: ['es', 'cjs'],
	minify: Boolean(process.env.MINIFY),
	platform: 'node',
	plugins: [replace({ preventAssignment: true, values: { ROCKSDB_JS_VERSION: version } })],
	sourcemap: true,
	tsconfig: './tsconfig.build.json',
});
export default config;
