import { Ivm } from './ivm.ts';
import { SqlClient } from './sql-client.ts';
import { TabSyncSimple } from '$lib/sync/syncers/tab-simple.ts';
import { SqlClientAdapter } from './sync/sql-client-adapter.ts';
import { DatabaseConnection } from './sync/database-connection.ts';
import { Syncer } from './sync/syncer.ts';
import { WsTransport } from './sync/transports/ws-transport.ts';
import { CrdtEvent, Source } from './sync/types.ts';
import type { StorageBackend } from './db/sqlite/types.ts';
import type { Tables } from './types.ts';

export class Store {
	static async create({
		path,
		type,
		schema,
		syncEndpoint
	}: {
		path: string;
		type: StorageBackend;
		schema: {
			tables: Tables;
			events: unknown;
			eventHandlers: Record<string, (store: Store, payload: unknown) => string>;
		};
		syncEndpoint?: string;
	}) {
		const { tables, events, eventHandlers } = schema;

		const db = new SqlClient({ path, backend: type, schema: tables });

		// @TODO schema.sql and migrations
		await db.run('CREATE TABLE IF NOT EXISTS todos(id TEXT UNIQUE, completed BOOL, text TEXT);');
		// @TODO and after this, we'd create the triggers `setup-crdt.ts`
		// if that's how we're going to handle it

		// For now, cache all tables
		const cache = await Ivm.using({ tables, db });

		// Initialize CRDT DatabaseConnection
		const dbAdapter = new SqlClientAdapter(db);
		const dbConn = await DatabaseConnection.init({
			db: dbAdapter,
			name: path,
			tables: Object.values(tables).map((t) => t.name)
		});

		// Initialize syncer with optional WebSocket transport
		let syncer: Syncer | undefined;
		if (syncEndpoint) {
			const ws = new WebSocket(syncEndpoint);
			const transport = new WsTransport(ws);
			syncer = await Syncer.init({ dbConn, transport });
		} else {
			syncer = await Syncer.init({ dbConn });
		}

		// Initialize tab syncer for cross-tab coordination
		const tabSync = new TabSyncSimple(`crdt-sync-${path}`);

		const store = new Store({ db, cache, eventHandlers, events, syncer, tabSync, dbConn });

		// Handle incoming events from other tabs - replay them through the same commit logic
		tabSync.onEvent((event: unknown) => {
			store.commitFromRemote(event as { name: string; payload: unknown; synced?: boolean });
		});

		// Handle incoming CRDT changes from remote peers
		if (syncer.peer) {
			syncer.peer.onUpdate(async (changedTables: Set<string>) => {
				// Refresh IVM for changed tables
				// @TODO, no, we want to recieve the event from the other tab
				// then replay it through our data. this is requerying the db
				// and replacing the IVM data
				await cache.refresh(Array.from(changedTables));
			});
		}

		return store;
	}

	cache: Ivm;
	eventHandlers: Record<string, (store: Store, payload: unknown) => string>;
	db: SqlClient;
	events: unknown;
	syncer: Syncer;
	tabSync: TabSyncSimple;
	dbConn: DatabaseConnection;

	constructor({
		db,
		cache,
		eventHandlers,
		events,
		syncer,
		tabSync,
		dbConn
	}: {
		db: SqlClient;
		cache: Ivm;
		eventHandlers: Record<string, (store: Store, payload: unknown) => string>;
		events: unknown;
		syncer: Syncer;
		tabSync: TabSyncSimple;
		dbConn: DatabaseConnection;
	}) {
		this.db = db;
		this.cache = cache;
		this.syncer = syncer;
		this.eventHandlers = eventHandlers;
		this.events = events;
		this.tabSync = tabSync;
		this.dbConn = dbConn;
	}

	// Process events from other tabs - only update IVM cache, not the database
	// Since tabs share the same OPFS SQLite database, the originating tab already wrote to it
	commitFromRemote(
		event: { name: string; payload: unknown; synced?: boolean },
		onSuccess: (result: unknown) => void = () => undefined,
		onError: (error: unknown) => void = () => undefined
	) {
		try {
			// Call event handler to update the IVM cache (has side effects)
			// We discard the SQL string since we don't write to the shared database
			this.eventHandlers[event.name](this, event.payload);

			onSuccess(null);
		} catch (err) {
			onError(err);
		}
	}

	// process events from this node
	commit(
		event: { name: string; payload: unknown; synced?: boolean },
		onSuccess: (result: unknown) => void = () => undefined,
		onError: (error: unknown) => void = () => undefined
	) {
		// match event to materailizer, instantiate a query
		const sql = this.eventHandlers[event.name](this, event.payload);

		// update the local UI optimistically and instantly
		// maybe this eventually looks something like this
		// const rollbackFn = this.cache.exec(query);
		const rollbackFn = () => this.cache.tables.todos.remove(event.payload);

		// persist the event to the db
		this.db.emit(sql, [], {
			success: async (result) => {
				// Broadcast event to other tabs (synchronous, non-blocking)
				if (this.tabSync) {
					this.tabSync.broadcastEvent(event);
				}
				this.db.emit(`SELECT * FROM crdt_changes`, [], {
					success: (r) => console.log('crdt_changes', r),
					failure: (err) => console.error('crdt_changes', err)
				});
				this.db.emit(`SELECT * FROM crdt_db_version`, [], {
					success: (r) => console.log('crdt_db_version', r),
					failure: (err) => console.error('crdt_db_version', err)
				});

				// Push CRDT changes to remote server
				if (event.synced && this.syncer.peer) {
					// Get all tracked table names from dbConn
					const watchedTables = new Set(this.dbConn.tables);

					// Get the last version we sent to the server
					const version = await this.dbConn.lastTrackedVersionFor(
						this.syncer.peer.serverSiteId as string,
						CrdtEvent.sent
					);

					// Push any new changes since that version
					await this.syncer.peer.pushChangesSince({ sinceVersion: version });

					// Notify the syncer about the change
					await this.syncer.sync({
						source: Source.UI,
						watchedTables
					});
				}

				onSuccess(result);
			},
			// if it fails, roll the cache and UI back
			failure: (err: unknown) => {
				// @TODO need to figure out how to rollback add/remove/update in IVM
				// would be nice if it could be computed without looking up and storing
				// previous value. `update` and `remove` are the tricky ones. `update` and `remove`
				// are the tricky ones. maybe this.cache.exec(query) returns {prev, current} or something?
				rollbackFn();
				onError(err);
			}
		});
	}
}
