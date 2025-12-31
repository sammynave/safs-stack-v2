import devtoolsJson from 'vite-plugin-devtools-json';
import { defineConfig } from 'vitest/config';
import { playwright } from '@vitest/browser-playwright';
import { sveltekit } from '@sveltejs/kit/vite';
import safsStackPlugin from './src/lib/db/sqlite/vite.js';

export default defineConfig({
	plugins: [sveltekit(), devtoolsJson(), safsStackPlugin()],

	test: {
		expect: { requireAssertions: true },

		projects: [
			{
				extends: './vite.config.ts',

				test: {
					name: 'client',

					browser: {
						enabled: true,
						provider: playwright(),
						instances: [{ browser: 'chromium', headless: true }]
					},

					include: ['src/**/*.svelte.{test,spec}.{js,ts}', 'src/**/*.{test,spec}.{js,ts}'],
					exclude: ['src/lib/server/**']
				}
			}

			// {
			// 	extends: './vite.config.ts',

			// 	test: {
			// 		name: 'server',
			// 		environment: 'node',
			// 		include: ['src/**/*.{test,spec}.{js,ts}'],
			// 		exclude: ['src/**/*.svelte.{test,spec}.{js,ts}']
			// 	}
			// }
		]
	}
});
