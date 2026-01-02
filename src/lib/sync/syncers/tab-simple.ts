/**
 * Simplified Tab Syncer for UI thread
 * Broadcasts events to other tabs so they can replay them through their IVM
 */

export class TabSyncSimple {
	private channel: BroadcastChannel;
	private eventHandler?: (event: any) => void;

	constructor(channelName: string) {
		this.channel = new BroadcastChannel(channelName);

		this.channel.onmessage = (message) => {
			const { event } = message.data;
			if (this.eventHandler) {
				this.eventHandler(event);
			}
		};
	}

	/**
	 * Broadcast an event to other tabs
	 * This is synchronous and non-blocking
	 */
	broadcastEvent(event: any): void {
		this.channel.postMessage({ event });
	}

	/**
	 * Register a handler for incoming events from other tabs
	 */
	onEvent(handler: (event: any) => void): void {
		this.eventHandler = handler;
	}

	/**
	 * Close the BroadcastChannel
	 */
	close(): void {
		this.channel.close();
	}
}
