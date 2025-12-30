import { Query } from '$lib/query.js';
import { store } from '../schema.ts';

// user called in UI
export const todosCount = Query.using(store)
	.from('todos')
	.select([[count('*')]]);
