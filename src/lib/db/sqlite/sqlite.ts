import type {
	DriverConfig,
	DriverStatement,
	EventId,
	RawResultData,
	SQLiteBackend,
	SQLValue
} from './types.ts';
import { SQLiteMemory } from './memory.ts';
import { SQLiteOPFS } from './opfs.ts';
import { SQLiteStorage } from './storage.ts';

export class SQLite {
	protected config?: DriverConfig;
	protected backend!: SQLiteBackend;

	setConfig(config: DriverConfig): void {
		this.config = config;

		switch (config.backend) {
			case 'memory':
				this.backend = new SQLiteMemory();
				break;
			case 'localStorage':
				this.backend = new SQLiteStorage('local');
				break;
			case 'sessionStorage':
				this.backend = new SQLiteStorage('session');
				break;
			case 'worker':
				this.backend = new SQLiteOPFS();
				break;
		}

		this.backend.setConfig(config);
	}

	emit(
		statement: DriverStatement,
		eventId: EventId,
		handlers: { success: (results: unknown) => void; failure: (error: unknown) => void }
	) {
		if (this.config?.backend !== 'worker') {
			throw Error(`Backend of type '${this.config?.backend}' does not support emitting sql`);
		}
		this.backend.emit(statement, eventId, handlers);
	}

	async exec(statement: DriverStatement): Promise<RawResultData> {
		return await this.backend.exec(statement);
	}

	async execBatch(statements: DriverStatement[]): Promise<RawResultData[]> {
		return await this.backend.execBatch(statements);
	}

	async transaction(statements: DriverStatement[]): Promise<RawResultData[]> {
		return await this.backend.transaction(statements);
	}

	async exportDatabase(): Promise<ArrayBuffer> {
		return await this.backend.exportDatabase();
	}

	async importDatabase(data: ArrayBuffer): Promise<void> {
		await this.backend.importDatabase(data);
	}

	async destroy(): Promise<void> {
		await this.backend.destroy();
	}

	get isReady(): boolean {
		return this.backend.isReady;
	}

	get hasPersistentStorage(): boolean {
		return this.backend.hasPersistentStorage;
	}
}
