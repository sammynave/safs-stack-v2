import { DatabaseBroadcast } from './comms/broadcast.ts';
import { mutexLock } from './comms/mutex.ts';
import type {
	DriverConfig,
	DriverStatement,
	EventId,
	RawResultData,
	SQLiteBackend,
	SQLValue
} from './types.ts';
import type {
	WorkerErrorResponse,
	WorkerMessage,
	WorkerResponse,
	WorkerSuccessResponse
} from './worker/types.ts';

export class SQLiteOPFS implements SQLiteBackend {
	private worker?: Worker;
	protected config?: DriverConfig;
	private messageId = 0;
	private pendingMessages = new Map<
		string,
		{
			resolve: (value: unknown) => void;
			reject: (error: Error) => void;
		}
	>();
	private pendingEmitMessages = new Map<string, WorkerMessage>();
	broadcast?: DatabaseBroadcast;
	private isInitialized = false;
	private initMutex = this.createInitMutex();

	setConfig(config: DriverConfig): void {
		this.config = config;
	}

	emit(
		statement: DriverStatement,
		eventId: EventId,
		handlers: { success: () => void; failure: () => void }
	) {
		if (!this.worker) {
			throw new Error('Worker not available');
		}

		const message = {
			type: 'emitExec',
			payload: statement
		};
		const fullMessage = { id: eventId, ...message } as WorkerMessage;
		this.pendingEmitMessages.set(eventId, { ...fullMessage, ...handlers });
		this.worker?.postMessage(fullMessage);
	}

	private createInitMutex() {
		let locked = false;
		const queue: Array<() => void> = [];

		return {
			async lock<T>(fn: () => Promise<T>): Promise<T> {
				while (locked) {
					await new Promise<void>((resolve) => queue.push(resolve));
				}
				locked = true;
				try {
					return await fn();
				} finally {
					locked = false;
					const next = queue.shift();
					if (next) next();
				}
			}
		};
	}

	private async ensureInit(): Promise<void> {
		if (this.isInitialized) return;

		await this.initMutex.lock(async () => {
			if (this.isInitialized) return;

			if (!this.config) {
				throw new Error('No configuration provided');
			}

			await this.init(this.config);
			this.isInitialized = true;
		});
	}

	private async init(config: DriverConfig): Promise<void> {
		if (typeof window === 'undefined') {
			throw new Error('Cannot run in Node.js environment');
		}

		if (typeof Worker === 'undefined') {
			throw new Error('Workers not available');
		}

		this.worker = new Worker(new URL('./worker/index.js', import.meta.url), {
			type: 'module'
		});

		this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
			const { id, success, type } = event.data;
			const pending =
				type === 'emit' ? this.pendingEmitMessages.get(id) : this.pendingMessages.get(id);

			if (pending) {
				if (type === 'emit') {
					this.pendingEmitMessages.delete(id);
					if (success) {
						// TODO this needs more abstraction
						// TODO this needs more abstraction
						// TODO this needs more abstraction
						// TODO this needs more abstraction
						const successResponse = event.data as WorkerSuccessResponse<unknown>;
						// can't import `todos` or `TodosRepo, causes circular dep
						pending.success(successResponse);
					} else {
						console.log('ERROR!!!!', event.data.error);
						const errorResponse = event.data as WorkerErrorResponse;
						// todos.remove();
						// TODO we'll want to thread this back to the UI
						pending.failure(errorResponse);
					}
				} else {
					this.pendingMessages.delete(id);
					if (success) {
						const successResponse = event.data as WorkerSuccessResponse<unknown>;
						pending.resolve(successResponse.result);
					} else {
						const errorResponse = event.data as WorkerErrorResponse;
						pending.reject(new Error(errorResponse.error));
					}
				}
			}
		};

		this.worker.onerror = (error) => {
			console.error('Worker error', error);
		};

		await this.sendToWorker<void>({
			type: 'init',
			payload: { databasePath: config.databasePath || '' }
		});

		if (config.databasePath) {
			this.broadcast = new DatabaseBroadcast(config.databasePath, {
				onReinit: async () => {
					console.log('Reinit broadcast received');
				},
				onClose: async () => {
					console.log('Close broadcast received');
				}
			});
		}
	}

	async exec(statement: DriverStatement): Promise<RawResultData> {
		await this.ensureInit();

		if (!this.worker) {
			throw new Error('Worker not available');
		}

		return await this.sendToWorker<RawResultData>({
			type: 'exec',
			payload: statement
		});
	}

	async execBatch(statements: DriverStatement[]): Promise<RawResultData[]> {
		await this.ensureInit();

		if (!this.worker) {
			throw new Error('Worker not available');
		}

		return await this.sendToWorker<RawResultData[]>({
			type: 'execBatch',
			payload: statements
		});
	}

	async transaction(statements: DriverStatement[]): Promise<RawResultData[]> {
		await this.ensureInit();

		if (!this.worker) {
			throw new Error('Worker not available');
		}

		return await this.sendToWorker<RawResultData[]>({
			type: 'transaction',
			payload: statements
		});
	}

	private async sendToWorker<T>(message: Omit<WorkerMessage, 'id'>): Promise<T> {
		if (!this.worker) {
			throw new Error('Worker not available');
		}

		const id = (++this.messageId).toString();

		return new Promise<T>((resolve, reject) => {
			const timeoutMs = this.getTimeoutForOperation(message.type);

			const timeoutId = setTimeout(() => {
				if (this.pendingMessages.has(id)) {
					this.pendingMessages.delete(id);
					reject(new Error(`Worker timeout after ${timeoutMs}ms for operation: ${message.type}`));
				}
			}, timeoutMs);

			this.pendingMessages.set(id, {
				resolve: (value: unknown) => {
					clearTimeout(timeoutId);
					resolve(value as T);
				},
				reject: (error: Error) => {
					clearTimeout(timeoutId);
					reject(error);
				}
			});

			const fullMessage = { id, ...message } as WorkerMessage;
			this.worker?.postMessage(fullMessage);
		});
	}

	private getTimeoutForOperation(type: string): number {
		switch (type) {
			case 'init':
				return 30000;
			case 'import':
				return 60000;
			case 'export':
				return 30000;
			case 'execBatch':
				return 15000;
			case 'transaction':
				return 15000;
			case 'exec':
				return 5000;
			case 'destroy':
				return 2000;
			default:
				return 10000;
		}
	}

	async exportDatabase(): Promise<ArrayBuffer> {
		await this.ensureInit();

		if (!this.worker || !this.config?.databasePath) {
			throw new Error('Export requires persistent storage');
		}

		return await mutexLock({ databasePath: this.config.databasePath, mode: 'shared' }, async () => {
			const result = await this.sendToWorker<{ name: string; data: ArrayBuffer }>({
				type: 'export',
				payload: undefined
			});

			return result.data;
		});
	}

	async importDatabase(data: ArrayBuffer): Promise<void> {
		await this.ensureInit();

		if (!this.worker || !this.config?.databasePath) {
			throw new Error('Import requires persistent storage');
		}

		return await mutexLock({ databasePath: this.config.databasePath }, async () => {
			this.broadcast?.broadcastClose();

			await this.sendToWorker<void>({
				type: 'import',
				payload: { data }
			});

			this.broadcast?.broadcastReinit();
		});
	}

	async destroy(): Promise<void> {
		for (const [_id, pending] of this.pendingMessages) {
			pending.reject(new Error('Worker destroyed while operation was pending'));
		}
		this.pendingMessages.clear();

		if (this.broadcast) {
			this.broadcast.broadcastClose();
			this.broadcast.close();
			this.broadcast = undefined;
		}

		if (this.worker) {
			this.worker.onmessage = null;
			this.worker.onerror = null;

			try {
				await this.sendToWorker<void>({
					type: 'destroy',
					payload: undefined
				});
			} catch {}

			this.worker.terminate();
			this.worker = undefined;
		}

		this.isInitialized = false;
	}

	get isReady(): boolean {
		return !!this.worker;
	}

	get hasPersistentStorage(): boolean {
		return !!this.worker;
	}
}
