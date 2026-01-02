/**
 * WebSocket transport for peer synchronization
 * Abstraction over WebSocket connection
 */

import type { Transport, TransportHandlers, MessageData } from '../types.ts';
import { MessageType } from '../types.ts';
import { encode, decode } from '../utils.ts';

/**
 * Generic WebSocket interface that consumers can implement
 */
export interface WebSocketLike {
	send(data: string | ArrayBuffer | Blob | ArrayBufferView): void;
	close(): void;
	addEventListener(type: 'open', listener: (event: Event) => void): void;
	addEventListener(type: 'close', listener: (event: Event) => void): void;
	addEventListener(type: 'error', listener: (event: Event) => void): void;
	addEventListener(type: 'message', listener: (event: MessageEvent) => void): void;
	removeEventListener(type: 'open', listener: (event: Event) => void): void;
	removeEventListener(type: 'close', listener: (event: Event) => void): void;
	removeEventListener(type: 'error', listener: (event: Event) => void): void;
	removeEventListener(type: 'message', listener: (event: MessageEvent) => void): void;
	readonly readyState: number;
}

/**
 * WebSocket transport implementation
 */
export class WsTransport implements Transport {
	ws: WebSocketLike;
	handlers: TransportHandlers = {};

	constructor(ws: WebSocketLike) {
		this.ws = ws;
	}

	/**
	 * Setup transport with message handlers
	 */
	async setup(handlers: TransportHandlers): Promise<void> {
		this.handlers = handlers;

		// Handle WebSocket events
		this.ws.addEventListener('error', this.handleError.bind(this));
		this.ws.addEventListener('open', this.handleOpen.bind(this));
		this.ws.addEventListener('message', this.handleMessage.bind(this));
	}

	/**
	 * Check if transport is ready to send messages
	 */
	isReady(): boolean {
		// WebSocket.OPEN = 1
		return this.ws.readyState === 1;
	}

	/**
	 * Send a message over the transport
	 */
	send(message: MessageData): void {
		if (!this.isReady()) {
			console.warn('WebSocket not ready, message not sent');
			return;
		}
		this.ws.send(encode(message));
	}

	/**
	 * Handle WebSocket errors
	 */
	private handleError(event: Event): void {
		console.error('WebSocket error:', event);
	}

	/**
	 * Handle WebSocket open event
	 */
	private handleOpen(_event: Event): void {
		// Connection established
		// Handlers will send initial messages if needed
	}

	/**
	 * Handle incoming WebSocket messages
	 */
	private async handleMessage(event: MessageEvent): Promise<void> {
		if (!event.data) return;

		try {
			// Handle different data formats
			let data: unknown;
			if (typeof event.data === 'string') {
				data = JSON.parse(event.data);
			} else if (event.data instanceof Blob) {
				const text = await event.data.text();
				data = JSON.parse(text);
			} else if (event.data instanceof ArrayBuffer) {
				data = decode(event.data);
			} else {
				data = decode(event.data);
			}

			const parsed = data as MessageData;

			// Route to appropriate handler based on message type
			switch (parsed.type) {
				case MessageType.connected:
					await this.handlers.onConnected?.(parsed);
					break;
				case MessageType.update:
					await this.handlers.onUpdate?.(parsed);
					break;
				case MessageType.ack:
					await this.handlers.onAck?.(parsed);
					break;
				case MessageType.pull:
					await this.handlers.onPull?.(parsed);
					break;
				default:
					console.warn('Unknown message type:', parsed.type);
			}
		} catch (error) {
			console.error('Failed to handle message:', error);
		}
	}

	/**
	 * Close the WebSocket connection
	 */
	close(): void {
		this.ws.close();
	}
}
