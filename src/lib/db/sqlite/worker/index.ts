import type { DriverStatement, RawResultData, SQLite, SQLiteDatabase } from '../types.ts';
import type {
	ExecBatchPayload,
	ExecPayload,
	ExportResult,
	ImportPayload,
	InitPayload,
	TransactionPayload,
	WorkerErrorResponse,
	WorkerMessage,
	WorkerSuccessResponse
} from './types.ts';

let sqlite: SQLite | undefined;
let db: SQLiteDatabase | undefined;
let isReady = false;

self.onmessage = async (event: MessageEvent<WorkerMessage>) => {
	// console.time('worker onmessage');
	const { id, type, payload } = event.data;
	try {
		switch (type) {
			case 'init': {
				await handleInit(payload);
				const response: WorkerSuccessResponse<void> = {
					id,
					success: true,
					result: undefined
				};
				self.postMessage(response);
				break;
			}
			case 'exec': {
				const result = await handleExec(payload);
				const response: WorkerSuccessResponse<RawResultData> = {
					id,
					success: true,
					result
				};
				self.postMessage(response);
				break;
			}
			case 'emitExec': {
				const result = await handleExec(payload);
				const response: WorkerSuccessResponse<RawResultData> = {
					id,
					success: true,
					type: 'emit',
					result
				};

				self.postMessage(response);
				break;
			}
			case 'execBatch': {
				const result = await handleExecBatch(payload);
				const response: WorkerSuccessResponse<RawResultData[]> = {
					id,
					success: true,
					result
				};
				self.postMessage(response);
				break;
			}
			case 'transaction': {
				const result = await handleTransaction(payload);
				const response: WorkerSuccessResponse<RawResultData[]> = {
					id,
					success: true,
					result
				};
				self.postMessage(response);
				break;
			}
			case 'export': {
				const result = await handleExport();
				const response: WorkerSuccessResponse<ExportResult> = {
					id,
					success: true,
					result
				};
				self.postMessage(response);
				break;
			}
			case 'import': {
				await handleImport(payload);
				const response: WorkerSuccessResponse<void> = {
					id,
					success: true,
					result: undefined
				};
				self.postMessage(response);
				break;
			}
			case 'destroy': {
				await handleDestroy();
				const response: WorkerSuccessResponse<void> = {
					id,
					success: true,
					result: undefined
				};
				self.postMessage(response);
				break;
			}
		}
	} catch (error) {
		const response: WorkerErrorResponse = {
			id,
			type: type === 'emitExec' ? 'emit' : type,
			success: false,
			error: `${error instanceof Error ? error.message : String(error)}`
		};
		console.warn('!!!!!!!!!!!!!!!!!IN WORKER error', response);
		self.postMessage(response);
	} finally {
		// console.timeEnd('worker onmessage');
	}
};

const handleInit = async (config: InitPayload): Promise<void> => {
	// console.time('import sqlite init');
	const { default: sqliteInitModule } = await import('@sqlite.org/sqlite-wasm');
	// console.timeEnd('import sqlite init');
	// console.time('sqlite init module');
	sqlite = await sqliteInitModule();
	// console.timeEnd('sqlite init module');

	if (!('storage' in navigator) || !('getDirectory' in navigator.storage)) {
		throw new Error('OPFS not supported in worker');
	}

	// console.time('worker assigning db from sqlite');
	try {
		db = new sqlite.oo1.DB(config.databasePath, 'cw', 'opfs-sahpool');
	} catch {
		try {
			db = new sqlite.oo1.OpfsDb(config.databasePath, 'cw');
		} catch {
			db = new sqlite.oo1.DB(config.databasePath, 'cw', 'opfs');
		}
	} finally {
		// console.timeEnd('worker assigning db from sqlite');
	}

	// console.time('set pragma');
	db.exec({ sql: 'PRAGMA journal_mode = WAL' });
	db.exec({ sql: 'PRAGMA synchronous = NORMAL' });
	db.exec({ sql: 'PRAGMA cache_size = -64000' });
	db.exec({ sql: 'PRAGMA foreign_keys = ON' });
	db.exec({ sql: 'PRAGMA temp_store = MEMORY' });
	db.exec({ sql: 'PRAGMA page_size = 8192' });
	db.exec({ sql: 'PRAGMA wal_autocheckpoint = 1000' });
	db.exec({ sql: 'PRAGMA busy_timeout = 5000' });
	// console.timeEnd('set pragma');

	isReady = true;
};

const handleExec = async (statement: ExecPayload): Promise<RawResultData> => {
	if (!isReady || !db) {
		throw new Error('Worker database not initialized');
	}

	return execOnDb(db, statement);
};

const handleExecBatch = async (statements: ExecBatchPayload): Promise<RawResultData[]> => {
	if (!isReady || !db) {
		throw new Error('Worker database not initialized');
	}

	return db.transaction('IMMEDIATE', (db) => {
		const results: RawResultData[] = [];
		for (const statement of statements) {
			results.push(execOnDb(db, statement));
		}
		return results;
	});
};

const handleTransaction = async (statements: TransactionPayload): Promise<RawResultData[]> => {
	if (!isReady || !db) {
		throw new Error('Worker database not initialized');
	}

	return db.transaction('IMMEDIATE', (db) => {
		const results: RawResultData[] = [];
		for (const statement of statements) {
			results.push(execOnDb(db, statement));
		}
		return results;
	});
};

const handleExport = async (): Promise<{ name: string; data: ArrayBuffer }> => {
	if (!sqlite || !db) {
		throw new Error('Worker database not initialized');
	}

	const exportedData = sqlite.capi.sqlite3_js_db_export(db);
	return {
		name: 'database.sqlite3',
		data: exportedData.buffer.slice(0)
	};
};

const handleImport = async (payload: ImportPayload): Promise<void> => {
	if (!sqlite || !db) {
		throw new Error('Worker database not initialized');
	}

	const dataArray = new Uint8Array(payload.data);
	const tempDbPath = `${db.filename}.import_temp`;

	try {
		await sqlite.oo1.OpfsDb.importDb(tempDbPath, dataArray);

		db.exec({ sql: `ATTACH DATABASE '${tempDbPath}' AS import_temp` });

		const tables = db.exec({
			sql: "SELECT name FROM import_temp.sqlite_master WHERE type='table' AND name != 'sqlite_sequence'",
			rowMode: 'array',
			returnValue: 'resultRows'
		}) as string[][];

		for (const [tableName] of tables) {
			db.exec({ sql: `DELETE FROM main."${tableName}"` });

			db.exec({
				sql: `INSERT INTO main."${tableName}" SELECT * FROM import_temp."${tableName}"`
			});
		}

		db.exec({ sql: 'DETACH DATABASE import_temp' });

		try {
			const root = await navigator.storage.getDirectory();
			await root.removeEntry(tempDbPath, { recursive: false });
		} catch (cleanupError) {
			console.warn('Failed to clean up temporary import file:', cleanupError);
		}
	} catch (error) {
		try {
			db.exec({ sql: 'DETACH DATABASE import_temp' });
		} catch {}

		try {
			const root = await navigator.storage.getDirectory();
			await root.removeEntry(tempDbPath, { recursive: false });
		} catch {}

		throw error;
	}
};

const handleDestroy = async (): Promise<void> => {
	if (db) {
		db.exec({ sql: 'PRAGMA optimize' });
		db.close();
		db = undefined;
	}

	isReady = false;
};

const execOnDb = (database: SQLiteDatabase, statement: DriverStatement): RawResultData => {
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
};
