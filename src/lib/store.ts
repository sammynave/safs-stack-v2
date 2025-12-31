import { Ivm } from './ivm.ts';
import { SqlClient } from './sql-client.ts';
import { Syncer } from './syncer.ts';

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

		return new Store({ db, cache, eventHandlers, events, syncer });
	}

	cache;
	eventHandlers;
	db;
	events;
	syncer;

	constructor({ db, cache, eventHandlers, events, syncer }) {
		this.db = db;
		this.cache = cache;
		this.syncer = syncer;
		this.eventHandlers = eventHandlers;
		this.events = events;
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
