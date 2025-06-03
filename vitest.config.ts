import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		allowOnly: true,
		coverage: {
			include: ['src/**/*.ts'],
			reporter: ['html', 'lcov', 'text']
		},
		environment: 'node',
		globals: false,
		include: ['test/**/*.test.ts'],
		pool: 'threads',
		poolOptions: {
			threads: {
				// NOTE: by default, Vitest will run tests in parallel, but
				// single threaded mode is useful for debugging:
				// singleThread: true
			}
		},
		reporters: ['verbose'],
		watch: false
	}
});