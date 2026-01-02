import { Ivm } from './ivm.ts';
import { SqlClient } from './sql-client.ts';
import { Syncer } from './syncer.ts';
import { TabSyncSimple } from '$lib/sync/syncers/tab-simple.ts';

export class Store {
	static async create({ path, type, schema }) {
		const { tables, events, eventHandlers } = schema;

		const db = new SqlClient({ path, backend: type, schema: tables });

		// @TODO schema.sql and migrations
		await db.run('CREATE TABLE IF NOT EXISTS todos(id TEXT UNIQUE, completed BOOL, text TEXT);');

		// For now, cache all tables
		const cache = await Ivm.using({ tables, db });

		const syncer = new Syncer({
			transport: 'ws',
			endpoint: '/sync',
			// Process events from other nodes
			onIncoming: (event) => {
				const sql = eventHandlers[event.name](this, event.payload);
				db.emit(sql, [], {
					success: (_result) => {
						cache.exec(changeQuery);
					},
					failure: (err) => {
						throw Error(`Error recieving sync emit: ${err}`);
					}
				});
			}
		});

		// Initialize tab syncer for cross-tab coordination
		const tabSync = new TabSyncSimple(`crdt-sync-${path}`);

		const store = new Store({ db, cache, eventHandlers, events, syncer, tabSync });

		// Handle incoming events from other tabs - replay them through the same commit logic
		tabSync.onEvent((event) => {
			store.commitFromRemote(event);
		});

		return store;
	}

	cache;
	eventHandlers;
	db;
	events;
	syncer;
	tabSync;

	constructor({ db, cache, eventHandlers, events, syncer, tabSync }) {
		this.db = db;
		this.cache = cache;
		this.syncer = syncer;
		this.eventHandlers = eventHandlers;
		this.events = events;
		this.tabSync = tabSync;
	}

	// Process events from other tabs - only update IVM cache, not the database
	// Since tabs share the same OPFS SQLite database, the originating tab already wrote to it
	commitFromRemote(
		event,
		onSuccess: (result: unknown) => undefined = (_result) => undefined,
		onError: (error: unknown) => undefined = (_error) => undefined
	) {
		try {
			// Call event handler to update the IVM cache (has side effects)
			// We discard the SQL string since we don't write to the shared database
			this.eventHandlers[event.name](this, event.payload);

			// Optionally sync to remote server (not local DB)
			if (event.synced) {
				this.syncer.sync(event, null);
			}

			onSuccess(null);
		} catch (err) {
			onError(err);
		}
	}

	// process events from this node
	commit(
		event,
		onSuccess: (result: unknown) => undefined = (_result) => undefined,
		onError: (error: unknown) => undefined = (_error) => undefined
	) {
		// match event to materailizer, instantiate a query
		const sql = this.eventHandlers[event.name](this, event.payload);

		// update the local UI optimistically and instantly
		// maybe this eventually looks something like this
		// const rollbackFn = this.cache.exec(query);
		const rollbackFn = () => this.cache.tables.todos.remove(event.payload);
		// persist the event to the db
		this.db.emit(sql, [], {
			success: (result) => {
				// `result` should include the metadata needed to sync
				// nodeId, hlc, etc...
				// look to https://github.com/sammynave/habits

				// Broadcast event to other tabs (synchronous, non-blocking)
				if (this.tabSync) {
					this.tabSync.broadcastEvent(event);
				}

				if (event.synced) {
					this.syncer.sync(event, result);
				}
				onSuccess(result);
			},
			// if it fails, roll the cache and UI back
			failure: (err) => {
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
