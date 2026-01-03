/**
 * Main Syncer class that orchestrates all sync layers
 */

import type { DatabaseConnection } from './database-connection.ts';
import type { Transport } from './types.ts';
import { Source } from './types.ts';
import { Tab } from './syncers/tab.ts';
import { Peer } from './syncers/peer.ts';
import { Ui } from './syncers/ui.ts';

/**
 * Syncer coordinates synchronization across three layers:
 * - UI: Reactive repositories with local database
 * - Tab: Cross-tab/window sync via BroadcastChannel
 * - Peer: Remote peer sync via WebSocket
 */
export class Syncer {
	static async init({
		dbConn,
		transport
	}: {
		dbConn: DatabaseConnection;
		transport?: Transport;
	}): Promise<Syncer> {
		const tab = new Tab({ dbConn });
		const ui = new Ui({ dbConn });

		const args: {
			dbConn: DatabaseConnection;
			ui: Ui;
			tab: Tab;
			peer?: Peer;
		} = { dbConn, tab, ui };

		// Only initialize peer layer if transport is provided
		if (transport) {
			args.peer = await Peer.init({ dbConn, transport });
		}

		return new Syncer(args);
	}

	dbConn: DatabaseConnection;
	tab: Tab;
	ui: Ui;
	peer?: Peer;

	private constructor({
		dbConn,
		tab,
		peer,
		ui
	}: {
		dbConn: DatabaseConnection;
		ui: Ui;
		tab: Tab;
		peer?: Peer;
	}) {
		this.dbConn = dbConn;
		this.tab = tab;
		this.peer = peer;
		this.ui = ui;

		// Wire up the sync method
		this.ui.sync = this.sync.bind(this);

		// Register tab sync handler
		// When we receive updates from other tabs, refresh local repos
		this.tab.onUpdate((data) => {
			this.sync({
				source: Source.TAB,
				watchedTables: data.watchedTables
			});
		});

		// Register peer sync handler (if peer exists)
		// When we receive updates from remote peers, refresh local repos and notify other tabs
		if (this.peer) {
			this.peer.onUpdate((tables: Set<string>) =>
				this.sync({ watchedTables: tables, source: Source.PEER })
			);
		}
	}

	/**
	 * Synchronize changes across layers
	 * Propagates updates based on the source:
	 * - UI changes → Tab + Peer
	 * - Tab changes → UI + Peer
	 * - Peer changes → UI + Tab
	 */
	async sync({
		source,
		watchedTables
	}: {
		source: Source;
		watchedTables: Set<string>;
	}): Promise<void> {
		/*
		 * Keep other tabs in sync
		 * Only post message to BroadcastChannel if sync was NOT triggered by a tab message
		 * Otherwise we're stuck in an infinite loop
		 */
		if (source === Source.UI || source === Source.PEER) {
			this.tab.update({ watchedTables });
		}

		/*
		 * Update local UI repos
		 * Refresh repos if changes came from tabs or peers
		 */
		if (source === Source.TAB || source === Source.PEER) {
			await this.ui.update({ watchedTables });
		}

		/*
		 * Push changes to remote peers
		 * Only if changes originated from local UI
		 */
		if (typeof this.peer !== 'undefined' && source === Source.UI) {
			// Push any updates made by this client to the server
			// NOTE: Don't await pushChangesSince, we don't want to block the UI
			// Any pending local changes will be pushed when the peer is ready
			const version = await this.dbConn.lastTrackedVersionFor(
				this.peer.serverSiteId as string,
				0 // CrdtEvent.sent
			);
			await this.peer.pushChangesSince({ sinceVersion: version });
		}
	}

	/**
	 * Close all sync connections
	 */
	close(): void {
		this.tab.close();
	}
}
