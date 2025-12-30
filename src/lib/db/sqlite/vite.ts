import type { Plugin, UserConfig } from 'vite';

export default function plugin(): Plugin<UserConfig> {
	return {
		name: 'vite-plugin-safs-stack-db',
		enforce: 'pre',
		config(config): UserConfig {
			return {
				optimizeDeps: {
					...config.optimizeDeps,
					exclude: [...(config.optimizeDeps?.exclude ?? []), '@sqlite.org/sqlite-wasm']
				},
				worker: {
					...config.worker,
					format: 'es'
				}
			};
		},
		configureServer(server): void {
			server.middlewares.use((_, res, next) => {
				res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
				res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
				next();
			});
		},

		configurePreviewServer(server): void {
			server.middlewares.use((_, res, next) => {
				res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
				res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
				next();
			});
		}
	};
}
