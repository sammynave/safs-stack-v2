import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/**
 * DistinctOperator - Removes duplicate rows (SQL DISTINCT)
 *
 * Stateful operator that tracks unique rows and their reference counts.
 * Only emits a row when it transitions from 0→1 occurrences (addition)
 * or from 1→0 occurrences (deletion).
 *
 * @example
 * ```typescript
 * const distinct = new DistinctOperator(source, (a, b) => {
 *   if (a.userId !== b.userId) return a.userId - b.userId;
 *   return a.amount - b.amount;
 * });
 * ```
 */
/* stateful */
export class DistinctOperator<T> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	readonly #comparator: Comparator<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;

	// Stores unique rows with their reference counts
	#uniqueRows: BTree<{ row: T; count: number }>;
	#initialized = false;

	constructor(source: Source<T>, comparator: Comparator<T>) {
		this.#source = source;
		this.#comparator = comparator;

		// Create a comparator for the stored objects that only compares the row
		const storageComparator = (a: { row: T; count: number }, b: { row: T; count: number }) => {
			return this.#comparator(a.row, b.row);
		};

		this.#uniqueRows = new BTree(storageComparator);
		this.#source.setSink(this);
	}

	get size() {
		return this.#uniqueRows.size;
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
			const probe = { row, count: 0 };
			const existing = this.#uniqueRows.get(probe);

			if (weight > 0) {
				// Addition
				if (existing) {
					// Row already exists, increment count
					this.#uniqueRows.delete(existing);
					existing.count += weight;
					this.#uniqueRows.add(existing);
					// Don't emit - row was already in output
				} else {
					// New unique row
					this.#uniqueRows.add({ row, count: weight });
					// Emit addition
					outputChangeSet.append([row, 1]);
				}
			} else if (weight < 0) {
				// Deletion
				if (existing) {
					this.#uniqueRows.delete(existing);
					existing.count += weight; // weight is negative

					if (existing.count > 0) {
						// Still have instances remaining
						this.#uniqueRows.add(existing);
						// Don't emit - row still in output
					} else if (existing.count === 0) {
						// Last instance removed
						// Emit deletion
						outputChangeSet.append([row, -1]);
					} else {
						// Count went negative - this shouldn't happen with proper usage
						console.warn('DistinctOperator: Reference count went negative', {
							row,
							count: existing.count
						});
					}
				}
				// If not existing, ignore the deletion (row wasn't in our set)
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#uniqueRows.clear();

			// Process initial data and build unique set
			for (const [row, weight] of this.#source.pull()) {
				if (weight > 0) {
					const probe = { row, count: 0 };
					const existing = this.#uniqueRows.get(probe);

					if (existing) {
						// Increment count for duplicate
						this.#uniqueRows.delete(existing);
						existing.count += weight;
						this.#uniqueRows.add(existing);
					} else {
						// New unique row
						this.#uniqueRows.add({ row, count: weight });
					}
				}
			}

			this.#initialized = true;
		}

		// Yield all unique rows
		for (const entry of this.#uniqueRows.values()) {
			yield [entry.row, 1] as [T, number];
		}
	}

	disconnect() {
		this.#uniqueRows.clear();
		this.#initialized = false;
		this.#source.disconnect(this);
	}
}
