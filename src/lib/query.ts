import { Query as Q } from './ivm/query/query.ts';

export class Query {
	static using(store) {
		return new Query(store);
	}
	store;
	cacheQueryBuilder;

	constructor(store) {
		this.store = store;
		this.cacheQueryBuilder = new Q(store.cache.tables);
	}
}
