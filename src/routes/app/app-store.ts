import { Store } from '$lib/store.ts';
import { schema } from './schema.ts';
import { generateTodosTriggers } from './setup-crdt.ts';

// user called
export const appStore = await Store.create({
	type: 'worker',
	path: 'safs-db',
	schema
});

// Install CRDT triggers to track changes for synchronization
const { insertTrigger, updateTrigger, deleteTrigger } = generateTodosTriggers();
await appStore.db.run(insertTrigger);
await appStore.db.run(updateTrigger);
await appStore.db.run(deleteTrigger);
