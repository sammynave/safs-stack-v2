/**
 * Peer syncer handles remote peer synchronization over WebSocket
 */

import type { DatabaseConnection } from '../database-connection.ts';
import type { Transport, Changes } from '../types.ts';
import { CrdtEvent, MessageType } from '../types.ts';

export class Peer {
	static async init({
		dbConn,
		transport
	}: {
		dbConn: DatabaseConnection;
		transport: Transport;
	}): Promise<Peer> {
		const peer = new Peer({ dbConn, transport });
		await transport.setup({
			onConnected: peer.handleConnected.bind(peer),
			onUpdate: peer.handleUpdate.bind(peer),
			onAck: peer.handleAck.bind(peer),
			onPull: peer.handlePull.bind(peer)
		});
		return peer;
	}

	dbConn: DatabaseConnection;
	transport: Transport;
	serverSiteId: string | undefined;
	hasData: boolean | undefined;
	pushQueue: Set<string> = new Set();
	queueing: Promise<void> | null = null;
	private updateFns: Set<(tables: Set<string>) => void> = new Set();

	private constructor({ dbConn, transport }: { dbConn: DatabaseConnection; transport: Transport }) {
		this.dbConn = dbConn;
		this.transport = transport;
	}

	get channel(): string {
		return this.dbConn.name;
	}

	/**
	 * Handle initial connection with server
	 */
	async handleConnected({ siteId }: { siteId: string; version: number }): Promise<void> {
		this.serverSiteId = siteId;
		const sinceVersion = await this.dbConn.lastTrackedVersionFor(siteId, CrdtEvent.sent);

		// Push any local changes to server
		await this.pushChangesSince({ sinceVersion });

		// Pull changes from server
		this.pullChangesSince({ version: await this.dbConn.getVersion() });
	}

	/**
	 * Request changes from peer since a specific version
	 */
	pullChangesSince({ version }: { version: number }): void {
		this.transport.send({
			channel: this.channel,
			type: MessageType.pull,
			siteId: this.dbConn.siteId,
			version
		});
	}

	/**
	 * Push local changes to peer since a specific version
	 */
	async pushChangesSince({ sinceVersion }: { sinceVersion: number }): Promise<void> {
		// Don't send if transport is not ready or we're offline
		if (!this.transport.isReady() || !navigator.onLine) {
			return;
		}

		const version = await this.dbConn.getVersion();
		const changes = await this.dbConn.changesSince(sinceVersion);

		// Queue changes to avoid sending duplicates
		for (const change of changes) {
			this.pushQueue.add(JSON.stringify(change));
		}

		// Batch changes using microtask queue
		if (this.queueing) {
			return this.queueing;
		}

		return (this.queueing = new Promise((resolve) =>
			queueMicrotask(() => {
				const batchedChanges: Changes = [];
				for (const serializedChange of this.pushQueue) {
					batchedChanges.push(JSON.parse(serializedChange));
					this.pushQueue.delete(serializedChange);
				}

				if (batchedChanges.length) {
					this.transport.send({
						channel: this.channel,
						type: MessageType.update,
						siteId: this.dbConn.siteId,
						version,
						changes: batchedChanges
					});
				}

				this.queueing = null;
				resolve();
			})
		));
	}

	/**
	 * Handle incoming changes from peer
	 */
	async handleUpdate(data: { changes?: Changes; version: number; siteId: string }): Promise<void> {
		const { changes, version, siteId } = data;

		if (!changes || changes.length === 0) {
			return;
		}
		// Check if we have any data (for initial bulk load optimization)
		if (typeof this.hasData === 'undefined') {
			// This is simplified - you may want a better way to check
			const currentVersion = await this.dbConn.getVersion();
			this.hasData = currentVersion > 0;
		}

		// Use bulk load for initial sync, regular merge for updates
		if (this.hasData) {
			await this.dbConn.merge(changes);
		} else {
			await this.dbConn.bulkLoad(changes);
			this.hasData = true;
		}

		// Track that we received these changes
		await this.dbConn.insertTrackedPeer(siteId, version, CrdtEvent.received);

		// Notify subscribers that tables changed
		const changedTables = new Set(changes.map(([table]) => table));
		await this.update(changedTables);
	}

	/**
	 * Handle acknowledgment from peer
	 */
	async handleAck({ siteId, version }: { siteId: string; version: number }): Promise<void> {
		const currentVersion = await this.dbConn.lastTrackedVersionFor(siteId, CrdtEvent.sent);

		// Update our tracking of what the peer has received
		if (currentVersion < version && this.serverSiteId) {
			await this.dbConn.insertTrackedPeer(this.serverSiteId, version, CrdtEvent.sent);
		}
	}

	/**
	 * Handle pull request from peer (typically server-side)
	 */
	async handlePull({ version: clientVersion }: { version: number }): Promise<void> {
		// This is typically handled server-side
		// Client-side implementation would be similar to pushChangesSince
		console.warn('handlePull called on client - this is typically server-side');
	}

	/**
	 * Register a callback for when updates are received
	 */
	onUpdate(fn: (tables: Set<string>) => void): void {
		this.updateFns.add(fn);
	}

	/**
	 * Notify all registered callbacks about table updates
	 */
	private async update(tables: Set<string>): Promise<void> {
		for (const fn of this.updateFns) {
			await fn(tables);
		}
	}
}
