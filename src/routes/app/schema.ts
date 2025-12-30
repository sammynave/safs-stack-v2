import { Query } from '$lib/query.js';
import { Store } from '$lib/store.js';
import type { Tables } from '$lib/types.js';

// essentialy DB schema
// this should follow StandardSchema
// https://standardschema.dev
export const tables: Tables = {
	todos: {
		name: 'todos',
		columns: {
			id: { type: 'text', primaryKey: true },
			text: { type: 'text', default: '' },
			completed: { type: 'boolean', default: false }
		}
		// @TODO indexes at some point
	}
};

// user created
export const events = {
	todoCreated: {
		name: 'v1.TodosCreated',
		schema: {
			// this should follow StandardSchema
			// https://standardschema.dev
			id: 'string',
			text: 'string'
		},
		synced: true
	},
	todoCompleted: {
		name: 'v1.TodoCompleted',
		schema: { id: 'string' },
		synced: true
	},
	todoUncompleted: {
		name: 'v1.TodoUncompleted',
		schema: { id: 'string' },
		synced: true
	},
	todoDeleted: {
		name: 'v1.TodoDeleted',
		schema: { id: 'string', deletedAt: 'date' },
		synced: true
	}
};

// user created
const eventHandlers = {
	'v1.TodoCreated': ({ id, text }: (typeof events)['todoCreated']['schema']) =>
		Query.insert('todos', { id, text, completed: false }),
	'v1.TodoCompleted': ({ id }: (typeof events)['todoCompleted']['schema']) =>
		Query.update('todos', { completed: true }).where({ id }),
	'v1.TodoUncompleted': ({ id }: (typeof events)['todoUncompleted']['schema']) =>
		Query.update('todos', { completed: false }).where({ id }),
	'v1.TodoDeleted': ({ id, deletedAt }: (typeof events)['todoDeleted']['schema']) =>
		Query.update('todos', { deletedAt }).where({ id })
};

// user called
export const store = new Store({
	tables,
	eventHandlers,
	type: 'worker',
	path: 'safs-db',
	events
});
