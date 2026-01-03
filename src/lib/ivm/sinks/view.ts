import type { ImmutableArray } from '$lib/immutable-types.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import type { Source } from '../stream-operators/stream-operator-types.ts';

/**
 * Internal type to track row multiplicity (how many times a row appears)
 */
type RowWithMultiplicity<T> = {
	row: T;
	count: number;
};

/**
 * View - A materialized view that supports duplicate rows
 *
 * Stores rows with their multiplicity count, allowing the same row
 * to appear multiple times (e.g., after projection or joins).
 */
export class View<T> {
	readonly #subscriptions = new Set<(data: ImmutableArray<T[]>) => void>();
	readonly #source: Source<T>;
	#data: BTree<RowWithMultiplicity<T>>;
	readonly #comparator: Comparator<T>;

	constructor(source: Source<T>, comparator: Comparator<T>) {
		this.#comparator = comparator;

		// Wrap the user's comparator to compare just the row part
		// This allows BTree to find rows by value equality
		const wrappedComparator = (a: RowWithMultiplicity<T>, b: RowWithMultiplicity<T>): number => {
			return comparator(a.row, b.row);
		};

		this.#data = new BTree(wrappedComparator);
		this.#source = source;
		this.#source.setSink(this);
	}

	/**
	 * Incremental update - processes a changeset from upstream operators
	 */
	push(changeSet: ChangeSet<T>) {
		let didChange = false;

		for (const [record, weight] of changeSet.data) {
			didChange = this.#updateMultiplicity(record, weight) || didChange;
		}

		if (didChange) {
			this.#notifyAll();
		}
	}

	/**
	 * Pull initial data from source and materialize the view
	 */
	materialize() {
		// Clear existing data
		this.#data.clear();

		// Pull from source and populate
		for (const [row, weight] of this.#source.pull()) {
			if (weight !== 0) {
				this.#updateMultiplicity(row, weight);
			}
		}

		return this.#toArray();
	}

	/**
	 * Returns the current state without re-materializing
	 */
	currentState() {
		return this.#toArray();
	}

	/**
	 * Subscribe to changes in the view
	 * Returns an unsubscribe function
	 */
	subscribe(subscription: (data: ImmutableArray<T[]>) => void) {
		this.#subscriptions.add(subscription);
		this.#notify(subscription);
		return () => {
			this.#subscriptions.delete(subscription);
		};
	}

	/**
	 * Disconnect from source and clean up
	 */
	disconnect() {
		this.#data.clear();
		this.#subscriptions.clear();
		this.#source.disconnect(this);
	}

	/**
	 * Update the multiplicity count for a row
	 * Returns true if the view changed
	 */
	#updateMultiplicity(record: T, weight: number): boolean {
		// Create a probe to find existing entry in BTree
		const probe: RowWithMultiplicity<T> = { row: record, count: 0 };
		const existing = this.#data.get(probe);

		if (existing) {
			// Row exists - update its count
			const newCount = existing.count + weight;

			// Remove old entry (BTree nodes are immutable)
			this.#data.delete(existing);

			if (newCount > 0) {
				// Re-add with updated count
				this.#data.add({ row: record, count: newCount });
			}
			// If newCount <= 0, we've removed it entirely (don't re-add)

			return true;
		} else if (weight > 0) {
			// New row - add it with initial count
			this.#data.add({ row: record, count: weight });
			return true;
		} else {
			// weight < 0 but row doesn't exist
			// This shouldn't happen in normal operation, but handle gracefully
			console.warn('Attempted to remove non-existent row:', record);
			return false;
		}
	}

	/**
	 * Convert BTree to array, expanding rows according to their multiplicity
	 */
	#toArray(): ImmutableArray<T> {
		const result: T[] = [];

		// Iterate through BTree in sorted order
		for (const entry of this.#data.values()) {
			// Expand each row according to its count
			// This creates the duplicate rows in the output
			for (let i = 0; i < entry.count; i++) {
				result.push(entry.row);
			}
		}

		return result as ImmutableArray<T>;
	}

	/**
	 * Notify a single subscription
	 */
	#notify(subscription: (data: ImmutableArray<T[]>) => void) {
		subscription(this.#toArray() as ImmutableArray<T[]>);
	}

	/**
	 * Notify all subscriptions
	 */
	#notifyAll() {
		for (const subscription of this.#subscriptions) {
			this.#notify(subscription);
		}
	}
}
