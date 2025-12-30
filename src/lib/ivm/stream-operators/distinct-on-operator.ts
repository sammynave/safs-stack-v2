import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/**
 * DistinctOnOperator - Incremental DISTINCT ON (SQL-like with continuous maintenance)
 *
 * Similar to PostgreSQL's DISTINCT ON, but with continuous maintenance:
 * - For each unique key, maintains the "best" row according to the row comparator
 * - When the "best" row is deleted, automatically promotes the next-best row
 * - This differs from PostgreSQL's snapshot semantics where the query result
 *   doesn't change until re-executed
 *
 * Useful for queries like:
 * - Most recent order per customer
 * - Highest bid per auction
 * - Latest login per user
 *
 * @example
 * ```typescript
 * // Keep the most recent order for each user
 * const distinctOn = new DistinctOnOperator(
 *   source,
 *   (row) => row.userId,                    // Key extractor
 *   (a, b) => a - b,                        // Key comparator
 *   (a, b) => b.timestamp - a.timestamp     // Row comparator (desc by timestamp)
 * );
 * ```
 */
/* stateful */
export class DistinctOnOperator<T, K> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	readonly #keyExtractor: (row: T) => K;
	readonly #keyComparator: Comparator<K>;
	readonly #rowComparator?: Comparator<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;

	// Maps each unique key to ALL its rows (sorted by rowComparator)
	// This enables fallback when the "best" row is removed
	#distinctRows: BTree<{ key: K; rows: BTree<T> }>;
	#initialized = false;

	constructor(
		source: Source<T>,
		keyExtractor: (row: T) => K,
		keyComparator: Comparator<K>,
		rowComparator?: Comparator<T>
	) {
		this.#source = source;
		this.#keyExtractor = keyExtractor;
		this.#keyComparator = keyComparator;
		this.#rowComparator = rowComparator;

		// Comparator that compares by key only
		const storageComparator = (a: { key: K; rows: BTree<T> }, b: { key: K; rows: BTree<T> }) => {
			return this.#keyComparator(a.key, b.key);
		};

		this.#distinctRows = new BTree(storageComparator);
		this.#source.setSink(this);
	}

	get size() {
		return this.#distinctRows.size;
	}

	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const outputChangeSet = new ChangeSet<T>([]);

		for (const [row, weight] of changeSet.data) {
			const key = this.#keyExtractor(row);
			const probe = { key, rows: null as any };
			const existing = this.#distinctRows.get(probe);

			if (weight > 0) {
				// Addition
				if (existing) {
					// Key already exists
					const oldBest = this.#getBestRow(existing.rows);

					// Add new row to the sorted collection
					this.#distinctRows.delete(existing);
					existing.rows.add(row);
					this.#distinctRows.add(existing);

					const newBest = this.#getBestRow(existing.rows);

					// Check if the best row changed
					if (!this.#rowsEqual(oldBest, newBest)) {
						// Emit deletion of old best, addition of new best
						if (oldBest) {
							outputChangeSet.append([oldBest, -1]);
						}
						outputChangeSet.append([newBest!, 1]);
					}
				} else {
					// New key - create new row collection
					const rowComparator = this.#rowComparator || this.#createDefaultComparator();
					const rows = new BTree<T>(rowComparator);
					rows.add(row);
					this.#distinctRows.add({ key, rows });
					outputChangeSet.append([row, 1]);
				}
			} else if (weight < 0) {
				// Deletion
				if (existing) {
					const oldBest = this.#getBestRow(existing.rows);

					// Remove row from the collection
					this.#distinctRows.delete(existing);
					existing.rows.delete(row);

					if (existing.rows.size > 0) {
						// Still have rows for this key
						this.#distinctRows.add(existing);

						const newBest = this.#getBestRow(existing.rows);

						// Check if the best row changed
						if (!this.#rowsEqual(oldBest, newBest)) {
							// Emit deletion of old best, addition of new best (fallback)
							if (oldBest) {
								outputChangeSet.append([oldBest, -1]);
							}
							if (newBest) {
								outputChangeSet.append([newBest, 1]);
							}
						}
					} else {
						// No more rows for this key - remove entirely
						if (oldBest) {
							outputChangeSet.append([oldBest, -1]);
						}
					}
				}
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}
	// TODO
	// TODO
	// TODO
	// i think `*pull` should always clear and pull from the source. get rid of the #initialized flag
	// if we need to, we can have a `snapshot` method that yields the state of the operator without pulling
	// from the chain
	*pull() {
		if (!this.#initialized) {
			this.#distinctRows.clear();

			// Process initial data
			for (const [row, weight] of this.#source.pull()) {
				if (weight > 0) {
					const key = this.#keyExtractor(row);
					const probe = { key, rows: null as any };
					const existing = this.#distinctRows.get(probe);

					if (existing) {
						// Key exists - add row to collection
						this.#distinctRows.delete(existing);
						existing.rows.add(row);
						this.#distinctRows.add(existing);
					} else {
						// New key - create new row collection
						const rowComparator = this.#rowComparator || this.#createDefaultComparator();
						const rows = new BTree<T>(rowComparator);
						rows.add(row);
						this.#distinctRows.add({ key, rows });
					}
				}
			}

			this.#initialized = true;
		}

		// Yield the best row for each key
		for (const entry of this.#distinctRows.values()) {
			const bestRow = this.#getBestRow(entry.rows);
			if (bestRow) {
				yield [bestRow, 1] as [T, number];
			}
		}
	}

	disconnect() {
		this.#distinctRows.clear();
		this.#initialized = false;
		this.#source.disconnect(this);
	}

	/**
	 * Gets the "best" row from a collection (first according to comparator)
	 */
	#getBestRow(rows: BTree<T>): T | undefined {
		for (const row of rows.values()) {
			return row; // Return first row (best according to comparator)
		}
		return undefined;
	}

	/**
	 * Compares two rows for equality using JSON serialization
	 */
	#rowsEqual(a: T | undefined, b: T | undefined): boolean {
		if (a === undefined && b === undefined) return true;
		if (a === undefined || b === undefined) return false;
		return JSON.stringify(a) === JSON.stringify(b);
	}

	/**
	 * Creates a default comparator that uses JSON serialization
	 * Used when no rowComparator is provided (keeps first row seen)
	 */
	#createDefaultComparator(): Comparator<T> {
		return (a: T, b: T) => {
			const jsonA = JSON.stringify(a);
			const jsonB = JSON.stringify(b);
			if (jsonA < jsonB) return -1;
			if (jsonA > jsonB) return 1;
			return 0;
		};
	}
}
