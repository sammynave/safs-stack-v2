export class Store {
	cache;
	eventHandlers;
	db;
	events;

	constructor({ tables, eventHandlers, type, path, events }) {
		this.cache = new Ivm(tables);
		this.db = new SqlClient({ path, backend: type, tables });
		this.eventHandlers = eventHandlers;
		this.events = events;

		this.syncer = new Syncer({
			transport: 'ws',
			endpoint: '/sync',
			// Process events from other nodes
			onIncoming: (event) => {
				const change = this.eventHandlers[event.name](event.payload);
				this.db.emit(change, {
					onSuccess: (_result) => {
						this.cache.exec(change);
					},
					onError: (err) => {
						throw Error(`Error recieving sync emit: ${err}`);
					}
				});
			}
		});
	}

	// process events from this node
	commit(event, onSuccess, onError) {
		// match event to materailizer, instantiate a query
		const change = this.eventHandlers[event.name](event.payload);

		// update the local UI optimistically and instantly
		this.cache.exec(change);

		// persist the event to the db
		this.db.emit(change, {
			onSuccess: (result) => {
				// `result` should include the metadata needed to sync
				// nodeId, hlc, etc...
				// look to https://github.com/sammynave/habits

				if (this.events[event.name].sync) {
					this.syncer.sync(event, result);
				}
				onSuccess(result);
			},
			// if it fails, roll the cache and UI back
			onError: (err) => {
				this.cache.rollback(query);
				onError(err);
			}
		});
	}
}
