import type { Database, Sqlite3Static } from '@sqlite.org/sqlite-wasm';
import type { DatabaseBroadcast } from './comms/broadcast.ts';

export type SQLite = Sqlite3Static;
export type SQLiteDatabase = Database;
export type SQLiteInitModule = () => Promise<Sqlite3Static>;
export type SQLiteMethod = 'get' | 'all' | 'run' | 'values';

export type SQLValue =
	| string
	| number
	| bigint
	| boolean
	| null
	| Uint8Array
	| ArrayBuffer
	| Int8Array;

export type SQLRow = Record<string, SQLValue>;

export interface RawResultData {
	columns: string[];
	rows: SQLValue[][] | SQLValue[];
}

export type DatabasePath = string;

export type StorageBackend = 'worker' | 'memory' | 'sessionStorage' | 'localStorage';

export interface DriverConfig {
	verbose?: boolean;
	readOnly?: boolean;
	databasePath: DatabasePath;
	backend?: StorageBackend;
}

export interface DriverStatement {
	sql: string;
	params?: SQLValue[];
	method?: SQLiteMethod;
}

export interface Statement {
	sql: string;
	params: SQLValue[];
}

export interface SQLiteBackend {
	setConfig(config: DriverConfig): void;
	exec(statement: DriverStatement): Promise<RawResultData>;
	execBatch(statements: DriverStatement[]): Promise<RawResultData[]>;
	transaction(statements: DriverStatement[]): Promise<RawResultData[]>;
	exportDatabase(): Promise<ArrayBuffer>;
	importDatabase(data: ArrayBuffer): Promise<void>;
	destroy(): Promise<void>;
	isReady: boolean;
	hasPersistentStorage: boolean;
	broadcast?: DatabaseBroadcast;
}

export type EventId = string;
