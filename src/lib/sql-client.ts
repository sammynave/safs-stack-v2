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
		// eslint-disable-next-line @typescript-eslint/no-unused-vars
		const _schemaPlaceholder = schema;
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

	async batch<T>(callback: (tx: BatchInterface) => T | Promise<T>): Promise<T> {
		return await this.client.batch(callback);
	}

	async transaction<T>(callback: (tx: TransactionInterface) => T | Promise<T>): Promise<T> {
		return await this.client.transaction(callback);
	}
}

interface BatchInterface {
	query<T = Record<string, SQLValue>>(sql: string, params?: SQLValue[]): Promise<T[]>;
	run(sql: string, params?: SQLValue[]): Promise<void>;
}

interface TransactionInterface {
	query<T = Record<string, SQLValue>>(sql: string, params?: SQLValue[]): Promise<T[]>;
	run(sql: string, params?: SQLValue[]): Promise<void>;
}
