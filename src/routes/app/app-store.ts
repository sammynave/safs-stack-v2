import { Store } from '$lib/store.ts';
import { schema } from './schema.ts';

// user called
export const appStore = await Store.create({
	type: 'worker',
	path: 'safs-db',
	schema
});
