import type { Store } from '$lib/store.ts';
import type { Tables } from '$lib/types.js';

// essentialy DB schema
// this should follow StandardSchema
// https://standardschema.dev
const tables: Tables = {
	todos: {
		name: 'todos',
		columns: {
			id: { type: 'text' },
			text: { type: 'text', default: '' },
			completed: { type: 'boolean', default: false }
		},
		primaryKey: 'id'
		// @TODO indexes at some point
	}
};

function event({ name, schema, synced }) {
	return (payload) => {
		// assert(validate(payload, schema))
		return { name, payload, synced, schema };
	};
}

// user created
const events = {
	todoCreated: event({
		name: 'v1.TodoCreated',
		schema: {
			// this should follow StandardSchema
			// https://standardschema.dev
			id: 'string',
			text: 'string'
		},
		synced: true
	}),
	todoCompleted: event({
		name: 'v1.TodoCompleted',
		schema: { id: 'string' },
		synced: true
	}),
	todoUncompleted: event({
		name: 'v1.TodoUncompleted',
		schema: { id: 'string' },
		synced: true
	}),
	todoDeleted: event({
		name: 'v1.TodoDeleted',
		schema: { id: 'string', deletedAt: 'date' },
		synced: true
	})
};
// let c = 0;
// user created
const eventHandlers = {
	'v1.TodoCreated': (
		store: Store,
		{ id, text }: ReturnType<(typeof events)['todoCreated']>['schema']
	) => {
		// @TODO we will want some abstraction over cache and persistent structures
		// like:
		//  `Query.using(store).insert({id, text, completed:false}).into('todos')`
		// then the query will call the correct IVM methods and SQlite methods
		// c++;
		store.cache.tables.todos.add({ id, text, completed: false });
		// if (c === 2) {
		// 	return `INSERT INTO todos (id, text, completed) VALUES ('${id}', ${text}, ${false});`;
		// } else {
		return `INSERT INTO todos (id, text, completed) VALUES ('${id}', '${text}', ${false});`;
		// }
	},
	'v1.TodoCompleted': (
		store: Store,
		{ id }: ReturnType<(typeof events)['todoCompleted']>['schema']
	) => {
		store.cache.tables.todos.update({ id }, { completed: true });
		return `UPDATE todos SET completed = 1 WHERE id = '${id}';`;
	},
	'v1.TodoUncompleted': (
		store: Store,
		{ id }: ReturnType<(typeof events)['todoUncompleted']>['schema']
	) => {
		store.cache.tables.todos.update({ id }, { completed: false });
		return `UPDATE todos SET completed = 0 WHERE id = '${id}';`;
	},
	'v1.TodoDeleted': (
		store: Store,
		{ id }: ReturnType<(typeof events)['todoDeleted']>['schema']
	) => {
		store.cache.tables.todos.remove({ id });
		return `DELETE FROM todos WHERE id = '${id}';`;
	}
};

export const schema = {
	tables,
	events,
	eventHandlers
};
