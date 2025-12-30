import { Client } from './db/sqlite/client.ts';
import type { SQLValue, StorageBackend } from './db/sqlite/types.ts';

export class SqlClient {
	client;
	constructor({
		path,
		backend,
		schema
	}: {
		path: string;
		backend: StorageBackend;
		schema: unknown;
	}) {
		// @TODO we'll want to ensure schema is correct
		this.client = new Client({ databasePath: path, backend });
	}

	async run(sql: string, params: SQLValue[] = []): Promise<void> {
		await this.client.run(sql, params);
	}

	async query<T = Record<string, SQLValue>>(sql: string, params: SQLValue[] = []): Promise<T[]> {
		return await this.client.query(sql, params);
	}

	emit(
		sql: string,
		params: SQLValue[] = [],
		handlers: { success: (results: unknown) => void; failure: (error: unknown) => void }
	) {
		this.client.emit(sql, params, handlers);
	}
}
