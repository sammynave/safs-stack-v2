export type BroadcastMessageType = 'reinit' | 'close' | 'update';

export interface BroadcastMessage {
	type: BroadcastMessageType;
	clientKey: string;
	timestamp: number;
}

export interface ReinitMessage extends BroadcastMessage {
	type: 'reinit';
}

export interface CloseMessage extends BroadcastMessage {
	type: 'close';
}
export interface UpdateMessage extends BroadcastMessage {
	type: 'update';
}

export type DatabaseBroadcastMessage = ReinitMessage | CloseMessage | UpdateMessage;

export interface BroadcastHandlers {
	onReinit?: () => void | Promise<void>;
	onClose?: () => void | Promise<void>;
}

export const createClientKey = (): string => {
	return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
};

export class DatabaseBroadcast {
	private channel: BroadcastChannel | null = null;
	private clientKey: string;
	private handlers: BroadcastHandlers = {};
	private databasePath: string;

	constructor(databasePath: string, handlers?: BroadcastHandlers) {
		this.databasePath = databasePath;
		this.clientKey = createClientKey();
		if (handlers) {
			this.handlers = handlers;
		}
		this.init();
	}

	send(message: Record<string | number, unknown>) {
		if (!this.channel) return;

		this.channel.postMessage({
			clientKey: this.clientKey,
			timestamp: Date.now(),
			...message
		});
	}

	private init(): void {
		// console.time('broadcast init');
		if (!this.isSupported()) {
			console.warn('BroadcastChannel not supported');
			return;
		}

		const channelName = `_safs_stack_db_(${this.databasePath})`;
		this.channel = new BroadcastChannel(channelName);

		this.channel.onmessage = (event: MessageEvent<DatabaseBroadcastMessage>) => {
			// console.time('onmessage');
			const message = event.data;

			if (message.clientKey === this.clientKey) {
				return;
			}

			void this.handleMessage(message);
			// console.timeEnd('onmessage');
		};

		this.channel.onmessageerror = (event) => {
			console.error('Broadcast message error', event);
		};
		// console.timeEnd('broadcast init');
	}

	private async handleMessage(message: DatabaseBroadcastMessage): Promise<void> {
		console.time('handleMessage', message);
		switch (message.type) {
			case 'reinit':
				if (this.handlers.onReinit) {
					await this.handlers.onReinit();
				}
				break;
			case 'close':
				if (this.handlers.onClose) {
					await this.handlers.onClose();
				}
				break;
			case 'update':
				this.handlers.onUpdate(message);
				break;
		}
		// console.timeEnd('handleMessage');
	}

	public broadcastReinit(): void {
		if (!this.channel) return;

		const message: ReinitMessage = {
			type: 'reinit',
			clientKey: this.clientKey,
			timestamp: Date.now()
		};

		this.channel.postMessage(message);
	}

	public broadcastClose(): void {
		if (!this.channel) return;

		const message: CloseMessage = {
			type: 'close',
			clientKey: this.clientKey,
			timestamp: Date.now()
		};

		this.channel.postMessage(message);
	}

	public setHandlers(handlers: BroadcastHandlers): void {
		this.handlers = { ...this.handlers, ...handlers };
	}

	public close(): void {
		if (this.channel) {
			this.channel.close();
			this.channel = null;
		}
	}

	public isSupported(): boolean {
		return typeof BroadcastChannel !== 'undefined';
	}

	public getClientKey(): string {
		return this.clientKey;
	}
}
