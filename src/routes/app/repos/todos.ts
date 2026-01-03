import { count } from '$lib/ivm/query/aggregation.js';
import { Query } from '$lib/query.js';
import { appStore } from '../app-store.ts';

// user called in UI
export const todosCount = Query.using(appStore)
	.cacheQueryBuilder.from('todos')
	.select([count('*')]);

export const allTodos = Query.using(appStore)
	.cacheQueryBuilder.from('todos')
	.limit(100)
	.select(['todos.id as id', 'todos.text as text', 'todos.completed as completed']);
