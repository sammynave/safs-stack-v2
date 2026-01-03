/**
 * Adapter to make SqlClient compatible with DatabaseInterface
 */

import type { SqlClient } from '../sql-client.ts';
import type { SQLValue } from '../db/sqlite/types.ts';
import type { DatabaseInterface } from './types.ts';

export class SqlClientAdapter implements DatabaseInterface {
	constructor(private client: SqlClient) {}

	/**
	 * Execute a SQL statement and return results
	 * Maps to client.query() for SELECT statements, client.run() for others
	 */
	async exec(sql: string, params?: unknown[]): Promise<unknown> {
		const trimmedSql = sql.trim().toUpperCase();

		// SELECT statements return rows
		if (trimmedSql.startsWith('SELECT')) {
			return await this.client.query(sql, (params as SQLValue[]) || []);
		}

		// All other statements (INSERT, UPDATE, DELETE, etc.) return void
		await this.client.run(sql, (params as SQLValue[]) || []);
		return [];
	}

	/**
	 * Execute multiple statements in a batch
	 */
	async execBatch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<unknown[]> {
		return await this.client.batch(
			async (tx: {
				query: (sql: string, params: SQLValue[]) => Promise<unknown>;
				run: (sql: string, params: SQLValue[]) => Promise<void>;
			}) => {
				const results: unknown[] = [];

				for (const stmt of statements) {
					const trimmedSql = stmt.sql.trim().toUpperCase();

					if (trimmedSql.startsWith('SELECT')) {
						const result = await tx.query(stmt.sql, (stmt.params as SQLValue[]) || []);
						results.push(result);
					} else {
						await tx.run(stmt.sql, (stmt.params as SQLValue[]) || []);
						results.push([]);
					}
				}

				return results;
			}
		);
	}

	/**
	 * Execute statements in a transaction
	 */
	async transaction(fn: (tx: DatabaseInterface) => Promise<void>): Promise<void> {
		await this.client.transaction(async (sqliteTx) => {
			// Create a DatabaseInterface wrapper for the transaction object
			const txAdapter: DatabaseInterface = {
				exec: async (sql: string, params?: unknown[]) => {
					const trimmedSql = sql.trim().toUpperCase();

					if (trimmedSql.startsWith('SELECT')) {
						return await sqliteTx.query(sql, (params as SQLValue[]) || []);
					}

					await sqliteTx.run(sql, (params as SQLValue[]) || []);
					return [];
				},
				execBatch: async (statements: Array<{ sql: string; params?: unknown[] }>) => {
					const results: unknown[] = [];

					for (const stmt of statements) {
						const result = await txAdapter.exec(stmt.sql, stmt.params);
						results.push(result);
					}

					return results;
				},
				transaction: async (nestedFn) => {
					// Nested transactions not supported - just execute in current transaction
					await nestedFn(txAdapter);
				}
			};

			await fn(txAdapter);
		});
	}
}
