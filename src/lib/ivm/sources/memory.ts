import { assert } from '$lib/asserts.js';
import { ChangeSet } from '$lib/ivm/change-set/change-set.js';
import { BTree } from '../b-plus-tree/tree.js';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operators/stream-operator-types.ts';

/*
 * The initial source of data
 * Responsible for keeping a copy of all the data,
 * receiving updates, and feeding deltas to
 * connected pipelines (i.e. reactive queries)
 *
 * NOTE: rewrite of forseti ReactiveTable
 */

export function defaultComparator(key: number | string) {
	return (
		a: Record<number | string, Record<string | number, unknown>>,
		b: Record<number | string, Record<string | number, unknown>>
	) => {
		if (a[key] === b[key]) return 0;
		if (a[key] === null || typeof a[key] === 'undefined') return -1;
		if (b[key] === null || typeof b[key] === 'undefined') return 1;
		if (typeof a[key] === 'boolean' && typeof b[key] === 'boolean') return a[key] ? 1 : -1;
		if (typeof a[key] === 'number' && typeof b[key] === 'number') {
			const result = a[key] - b[key];
			return result;
		}
		if (typeof a[key] === 'string' && typeof b[key] === 'string') {
			const result = a[key].localeCompare(b[key]);
			return result;
		}
		throw Error(`typeof a: ${typeof a} and/or typeof b: ${typeof b} not supported`);
	};
}

interface Index<Row> {
	data: BTree<Row>;
	comparator: (a: unknown, b: unknown) => 0 | 1 | -1;
	usedBy: Set<Sink<Row>>;
}
export class Memory<Row> implements Source<Row> {
	readonly #connections: Sink<Row>[] = [];
	readonly #pk: string;
	readonly #pkIndexSort: [string, 'asc' | 'desc'];
	readonly schema: unknown;
	#indexes: Map<string, Index<Row>> = new Map();

	/*
	 * NOTE: might want to pass in schema rather than pk
	 */
	constructor({ initialData, pk, schema }: { initialData: Row[]; pk: string; schema: unknown }) {
		this.#pk = pk;
		this.schema = schema;

		const comparator = defaultComparator(pk);
		const pkIndex = new BTree(comparator);
		for (const row of initialData) {
			pkIndex.add(row);
		}
		this.#pkIndexSort = [pk, 'asc'];
		const usedBy = new Set();
		usedBy.add(NullSink);
		this.#indexes.set(JSON.stringify(this.#pkIndexSort), {
			comparator,
			data: pkIndex,
			usedBy
		});
	}
	get #data() {
		return this.#indexes.get(JSON.stringify(this.#pkIndexSort)).data;
	}
	get size() {
		return this.#data.size;
	}

	connect(
		sort: [string, 'asc' | 'desc'] = this.#pkIndexSort,
		comparator: ((a: unknown, b: unknown) => 0 | 1 | -1) | null = null
	) {
		const index = this.#newIndex(comparator, sort);
		const connections = () => this.#connections;
		const disconnect = (query) => this.disconnect(query);
		return {
			setSink(sink) {
				index?.usedBy.add(sink);
				connections().push(sink);
			},
			*pull() {
				for (const row of index?.data.values()) {
					yield [row, 1] as [Row, number];
				}
			},
			disconnect
		};
	}

	#newIndex(comparator: (a: unknown, b: unknown) => 0 | 1 | -1, sort: [string, 'asc' | 'desc']) {
		const sortKey = JSON.stringify(sort);
		if (this.#indexes.has(sortKey)) {
			const index = this.#indexes.get(sortKey);
			return index;
		} else {
			const newIndexData = new BTree(comparator);

			for (const d of this.#data.values()) {
				newIndexData.add(d);
			}

			const newIndex = { comparator, data: newIndexData, usedBy: new Set() };
			this.#indexes.set(sortKey, newIndex);
			return newIndex;
		}
	}

	// TODO infer Row from this.schema
	add(row: Row): void {
		assert(
			!this.#data.has(row),
			Error(`Record with id ${row} already exists. Use update() instead.`)
		);
		for (const [sort, { data }] of this.#indexes) {
			data.add(row);
		}

		// Pass the delta to our connected queries
		this.#process(new ChangeSet([[row, 1]]));
	}

	/**
	 * Updates a row in the memory source by its primary key.
	 *
	 * @param row - An object containing at least the primary key field for lookup.
	 *              Only the primary key is used to find the row; other fields are ignored.
	 * @param changes - Partial row object with fields to update
	 *
	 * @throws {Error} If no row with the given primary key exists
	 */
	// TODO expect this not to work.
	// TODO need to update all indexes too
	update(row: Row, changes: Partial<Row>): void {
		const oldItem = this.#data.get(row);
		assert(Boolean(oldItem), Error(`Record with id ${row} not found. Use add() instead.`));

		const newItem = { ...oldItem, ...changes } as Row;
		this.#data.add(newItem);
		const delta = new ChangeSet([
			[oldItem, -1], // Remove old
			[newItem, 1] // Add new
		]);
		this.#process(delta);
	}

	/**
	 * Removes a row from the memory source by its primary key.
	 *
	 * @param row - An object containing at least the primary key field.
	 *              Only the primary key is used for lookup; other fields are ignored.
	 *              For example, if pk='id', you can pass `{ id: 1 }` or `{ id: 1, amount: 999 }`
	 *              and it will remove the row where id=1, regardless of other field values.
	 *
	 * @throws {Error} If no row with the given primary key exists
	 *
	 * @example
	 * // If the table has { id: 1, amount: 10 }
	 * memory.remove({ id: 1 });              // ✓ Works - removes the row
	 * memory.remove({ id: 1, amount: 999 }); // ✓ Also works - amount is ignored
	 */
	remove(row: Row): void {
		const item = this.#data.get(row);
		assert(Boolean(item), Error(`Can not delete; id ${row} not found!`));
		for (const [sort, { data }] of this.#indexes) {
			data.delete(row);
		}

		// Create delta and notify subscribers
		this.#process(new ChangeSet([[item, -1]]));
	}

	disconnect(query: Sink<Row>) {
		const idx = this.#connections.findIndex((q) => q === query);
		assert(idx !== -1, Error(`Query not found`, { cause: query }));
		// TODO if no one is using the index, delete it
		this.#connections.splice(idx, 1);
	}

	#process(delta) {
		for (const conn of this.#connections) {
			conn.push(delta);
		}
	}
}
