import typescript from '@rollup/plugin-typescript';
import { defineConfig } from 'rollup';
import { minify as esbuildMinifyPlugin } from 'rollup-plugin-esbuild';
import replace from '@rollup/plugin-replace';
import { readFileSync } from 'node:fs';
import commonjs from '@rollup/plugin-commonjs';
import { nodeResolve } from '@rollup/plugin-node-resolve';

const { version } = JSON.parse(readFileSync('./package.json', 'utf8'));

export default defineConfig([
	{
		input: './src/index.ts',
		output: {
			dir: './dist',
			externalLiveBindings: false,
			format: 'es',
			freeze: false,
			preserveModules: false,
			sourcemap: true
		},
		plugins: [
			esbuildMinifyPlugin({
				minify: true,
				minifySyntax: true
			}),
			typescript({
				tsconfig: './tsconfig.build.json'
			}),
			replace({
				preventAssignment: true,
				values: {
					'ROCKSDB_JS_VERSION': version
				}
			}),
			nodeResolve(),
			commonjs()
		]
	}
]);
