import { Memory } from './ivm/sources/memory.ts';
import type { Tables } from './types.ts';

interface DbLike {
	query(sql: string, params?: unknown[]): Promise<unknown[]>;
}

export class Ivm {
	static async using({ tables, db }: { tables: Tables; db: DbLike }) {
		const tableNames = Object.values(tables).map(({ name }) => name);
		const ivmTables: Record<string, Memory<unknown>> = {};

		// @TODO probably a faster way to do this
		for (const tableName of tableNames) {
			const pk = tables[tableName].primaryKey;
			ivmTables[tableName] = new Memory({
				initialData: await db.query(`SELECT * FROM ${tableName};`),
				schema: tables[tableName],
				pk
			});
		}

		return new Ivm(ivmTables, tables, db);
	}

	tables: Record<string, Memory<unknown>>;
	private tableSchemas: Tables;
	private db: DbLike;

	constructor(tables: Record<string, Memory<unknown>>, tableSchemas: Tables, db: DbLike) {
		this.tables = tables;
		this.tableSchemas = tableSchemas;
		this.db = db;
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	execute(query: any) {
		// this will `add`,`remove`,`update` the `Memory` source
		// and return a "rollback" query
		return query.using(this.tables).execute();
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	rollback(query: any) {
		query.using(this.tables).rollback();
	}

	/**
	 * Refresh IVM data for specific tables from the database
	 * Used when remote changes are received
	 */
	async refresh(tableNames: string[]) {
		for (const tableName of tableNames) {
			if (this.tables[tableName]) {
				const freshData = await this.db.query(`SELECT * FROM ${tableName};`);
				// Reset the Memory source with fresh data
				this.tables[tableName] = new Memory({
					initialData: freshData,
					schema: this.tableSchemas[tableName],
					pk: this.tableSchemas[tableName].primaryKey
				});
			}
		}
	}
}
