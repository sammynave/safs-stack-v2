import type {
	DriverConfig,
	DriverStatement,
	RawResultData,
	SQLite,
	SQLiteBackend,
	SQLiteDatabase
} from './types.ts';

export class SQLiteStorage implements SQLiteBackend {
	private sqlite?: SQLite;
	private db?: SQLiteDatabase;
	private isInitialized = false;
	protected config?: DriverConfig;
	private storageType: 'local' | 'session';

	constructor(storageType: 'local' | 'session') {
		this.storageType = storageType;
	}

	setConfig(config: DriverConfig): void {
		this.config = config;
	}

	async init(): Promise<void> {
		if (this.isInitialized) return;

		const { default: sqliteInitModule } = await import('@sqlite.org/sqlite-wasm');
		this.sqlite = await sqliteInitModule();

		this.db = new this.sqlite.oo1.JsStorageDb(this.storageType);

		this.db.exec({ sql: 'PRAGMA journal_mode = MEMORY' });
		this.db.exec({ sql: 'PRAGMA synchronous = OFF' });
		this.db.exec({ sql: 'PRAGMA cache_size = -16000' });
		this.db.exec({ sql: 'PRAGMA foreign_keys = ON' });
		this.db.exec({ sql: 'PRAGMA temp_store = MEMORY' });

		this.isInitialized = true;
	}

	async exec(statement: DriverStatement): Promise<RawResultData> {
		await this.init();

		if (!this.db) {
			throw new Error('Database not initialized');
		}

		return this.execOnDb(this.db, statement);
	}

	async execBatch(statements: DriverStatement[]): Promise<RawResultData[]> {
		await this.init();

		if (!this.db) {
			throw new Error('Database not initialized');
		}

		return this.db.transaction('IMMEDIATE', (db) => {
			const results: RawResultData[] = [];
			for (const statement of statements) {
				results.push(this.execOnDb(db, statement));
			}
			return results;
		});
	}

	async transaction(statements: DriverStatement[]): Promise<RawResultData[]> {
		await this.init();

		if (!this.db) {
			throw new Error('Database not initialized');
		}

		return this.db.transaction('IMMEDIATE', (db) => {
			const results: RawResultData[] = [];
			for (const statement of statements) {
				results.push(this.execOnDb(db, statement));
			}
			return results;
		});
	}

	private execOnDb(database: SQLiteDatabase, statement: DriverStatement): RawResultData {
		const result: RawResultData = { rows: [], columns: [] };

		if (statement.method === 'run') {
			database.exec({
				sql: statement.sql,
				bind: statement.params || []
			});
		} else {
			const rows = database.exec({
				rowMode: 'array',
				sql: statement.sql,
				bind: statement.params || [],
				returnValue: 'resultRows',
				columnNames: result.columns
			});

			if (statement.method === 'get') {
				result.rows = rows[0] ? [rows[0]] : [];
			} else {
				result.rows = rows;
			}
		}

		return result;
	}

	async exportDatabase(): Promise<ArrayBuffer> {
		if (!this.sqlite || !this.db) {
			throw new Error('Database not initialized');
		}

		const exportedData = this.sqlite.capi.sqlite3_js_db_export(this.db);
		return exportedData.buffer.slice(0);
	}

	async importDatabase(data: ArrayBuffer): Promise<void> {
		if (!this.sqlite || !this.db) {
			throw new Error('Database not initialized');
		}

		this.db.close();

		const dataArray = new Uint8Array(data);
		this.db = new this.sqlite.oo1.DB();

		if (!this.db.pointer) {
			throw new Error('Failed to get database pointer');
		}

		const pData = this.sqlite.wasm.allocFromTypedArray(dataArray);

		const rc = this.sqlite.capi.sqlite3_deserialize(
			this.db.pointer,
			'main',
			pData,
			dataArray.byteLength,
			dataArray.byteLength,
			this.sqlite.capi.SQLITE_DESERIALIZE_FREEONCLOSE
		);

		this.db.checkRc(rc);
	}

	async destroy(): Promise<void> {
		if (this.db) {
			this.db.close();
			this.db = undefined;
		}

		this.isInitialized = false;
	}

	get isReady(): boolean {
		return this.isInitialized && !!this.db;
	}

	get hasPersistentStorage(): boolean {
		return this.storageType === 'local';
	}
}
