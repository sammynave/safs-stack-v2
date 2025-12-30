import { Memory } from './ivm/sources/memory.ts';
import type { Tables } from './types.ts';

export class Ivm {
	static async using({ tables, db }: { tables: Tables; db: unknown }) {
		const tableNames = Object.values(tables).map(({ name }) => name);
		const ivmTables: Record<(typeof tableNames)[number], Array<unknown>> = {};

		// @TODO probably a faster way to do this
		for (const tableName of tableNames) {
			const pk = tables[tableName].primaryKey;
			ivmTables[tableName] = new Memory({
				initialData: await db.query(`SELECT * FROM ${tableName};`),
				schema: tables[tableName],
				pk
			});
		}

		return new Ivm(ivmTables);
	}

	tables;

	constructor(tables) {
		this.tables = tables;
	}

	execute(query) {
		// this will `add`,`remove`,`update` the `Memory` source
		// and return a "rollback" query
		return query.using(this.tables).execute();
	}
	rollback(query) {
		query.using(this.tables).rollback();
	}
}
