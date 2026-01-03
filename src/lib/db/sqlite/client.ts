import { SQLite } from './sqlite.ts';
import type { DriverConfig, EventId, SQLValue } from './types.ts';

export class Client {
	private db: SQLite;

	constructor(config: DriverConfig) {
		// console.time('create sqlite instance');
		this.db = new SQLite();
		// console.timeEnd('create sqlite instance');
		// console.time('set sqlite config');
		this.db.setConfig(config);
		// console.timeEnd('set sqlite config');
	}

	emit(
		sql: string,
		params: SQLValue[] = [],
		handlers: { success: (results: unknown) => void; failure: (error: unknown) => void }
	) {
		const eventId = crypto.randomUUID();
		this.db.emit({ sql, params, method: 'all' }, eventId, handlers);
	}

	broadcast(message) {
		// console.time('broadcast message');
		if (this?.db?.backend?.broadcast?.send) {
			this.db.backend.broadcast.send(message);
		}
		// console.timeEnd('broadcast message');
	}
	get clientKey() {
		return this.db.backend.broadcast?.getClientKey();
	}

	async sql<T = Record<string, SQLValue>>(
		queryTemplate: TemplateStringsArray | string,
		...params: SQLValue[]
	): Promise<T[]> {
		const sql = this.buildQuery(queryTemplate, params);
		const result = await this.db.exec({ sql, params: [], method: 'all' });

		return this.convertToObjects<T>(result);
	}

	async query<T = Record<string, SQLValue>>(sql: string, params: SQLValue[] = []): Promise<T[]> {
		const result = await this.db.exec({ sql, params, method: 'all' });
		return this.convertToObjects<T>(result);
	}

	async get<T = Record<string, SQLValue>>(
		sql: string,
		params: SQLValue[] = []
	): Promise<T | undefined> {
		const result = await this.db.exec({ sql, params, method: 'get' });
		const objects = this.convertToObjects<T>(result);
		return objects[0];
	}

	async run(sql: string, params: SQLValue[] = []): Promise<void> {
		await this.db.exec({ sql, params, method: 'run' });
	}

	async batch<T>(callback: (tx: BatchInterface) => T | Promise<T>): Promise<T> {
		const statements: Array<{ sql: string; params: SQLValue[] }> = [];

		const tx: BatchInterface = {
			sql: async (queryTemplate, ...params) => {
				const sql = this.buildQuery(queryTemplate, params);
				statements.push({ sql, params: [] });
				return [];
			},
			query: async (sql, params = []) => {
				statements.push({ sql, params });
				return [];
			},
			run: async (sql, params = []) => {
				statements.push({ sql, params });
			}
		};

		const result = await callback(tx);

		if (statements.length > 0) {
			const driverStatements = statements.map((stmt) => ({
				sql: stmt.sql,
				params: stmt.params,
				method: 'run' as const
			}));

			await this.db.execBatch(driverStatements);
		}

		return result;
	}

	async transaction<T>(callback: (tx: TransactionInterface) => T | Promise<T>): Promise<T> {
		await this.db.exec({ sql: 'BEGIN IMMEDIATE', params: [], method: 'run' });

		try {
			const tx: TransactionInterface = {
				sql: async (queryTemplate, ...params) => {
					const sql = this.buildQuery(queryTemplate, params);
					const result = await this.db.exec({ sql, params: [], method: 'all' });
					return this.convertToObjects(result);
				},
				query: async (sql, params = []) => {
					const result = await this.db.exec({ sql, params, method: 'all' });
					return this.convertToObjects(result);
				},
				run: async (sql, params = []) => {
					await this.db.exec({ sql, params, method: 'run' });
				}
			};

			const result = await callback(tx);
			await this.db.exec({ sql: 'COMMIT', params: [], method: 'run' });
			return result;
		} catch (error) {
			await this.db.exec({ sql: 'ROLLBACK', params: [], method: 'run' });
			throw error;
		}
	}

	get status() {
		return {
			ready: this.db.isReady,
			persistent: this.db.hasPersistentStorage
		};
	}

	async exportDatabase(): Promise<ArrayBuffer> {
		return await this.db.exportDatabase();
	}

	async importDatabase(data: ArrayBuffer): Promise<void> {
		await this.db.importDatabase(data);
	}

	async close(): Promise<void> {
		await this.db.destroy();
	}

	private buildQuery(queryTemplate: TemplateStringsArray | string, params: SQLValue[]): string {
		if (typeof queryTemplate === 'string') {
			return queryTemplate;
		}

		let sql = queryTemplate[0] || '';
		for (let i = 0; i < params.length; i++) {
			sql += `?${queryTemplate[i + 1] || ''}`;
		}
		return sql;
	}

	private convertToObjects<T = Record<string, SQLValue>>(result: {
		rows: SQLValue[][] | SQLValue[];
		columns: string[];
	}): T[] {
		if (!Array.isArray(result.rows)) {
			return [];
		}

		const objects: T[] = [];

		for (const row of result.rows) {
			if (Array.isArray(row)) {
				const obj = {} as T;
				for (let i = 0; i < result.columns.length; i++) {
					const column = result.columns[i];
					if (column) {
						const key = column as keyof T;
						obj[key] = row[i] as T[keyof T];
					}
				}
				objects.push(obj);
			}
		}

		return objects;
	}
}

interface BatchInterface {
	sql<T = Record<string, SQLValue>>(
		queryTemplate: TemplateStringsArray | string,
		...params: SQLValue[]
	): Promise<T[]>;
	query<T = Record<string, SQLValue>>(sql: string, params?: SQLValue[]): Promise<T[]>;
	run(sql: string, params?: SQLValue[]): Promise<void>;
}

interface TransactionInterface {
	sql<T = Record<string, SQLValue>>(
		queryTemplate: TemplateStringsArray | string,
		...params: SQLValue[]
	): Promise<T[]>;
	query<T = Record<string, SQLValue>>(sql: string, params?: SQLValue[]): Promise<T[]>;
	run(sql: string, params?: SQLValue[]): Promise<void>;
}
