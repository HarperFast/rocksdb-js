import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		coverage: {
			include: ['src/**/*.ts'],
			reporter: ['html', 'lcov', 'text']
		},
		environment: 'node',
		globals: false,
		include: ['test/**/*.test.ts'],
		poolOptions: {
			forks: {
				execArgv: ['--expose-gc'],
				singleFork: true
			}
		},
		watch: false
	}
});