import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../../b-plus-tree/tree.ts';
import { ChangeSet } from '../../change-set/change-set.ts';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

type CountGroupResult<T> = Partial<T> & { count: number };

/* stateful */
export class CountGroupByOperator<T> implements Sink<GroupedRow<T>>, Source<CountGroupResult<T>> {
	readonly #source: Source<GroupedRow<T>>;
	#sink: Sink<CountGroupResult<T>> | typeof NullSink = NullSink;
	// Use BTree instead of Map for deterministic ordering
	#groupCounts: BTree<{ keyValues: Partial<T>; count: number }>;
	#initialized = false;

	constructor(source: Source<GroupedRow<T>>) {
		this.#source = source;
		this.#source.setSink(this);

		// Initialize BTree with a comparator that sorts by keyValues string representation
		this.#groupCounts = new BTree(this.#createGroupResultComparator());
	}

	get size() {
		return this.#groupCounts.size;
	}

	setSink(sink: Sink<CountGroupResult<T>>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<GroupedRow<T>>) {
		const outputChangeSet = new ChangeSet<CountGroupResult<T>>([]);

		// Single pass processing
		for (const [group, weight] of changeSet.data) {
			if (weight === 0) continue;

			// We need to find the existing entry in the BTree.
			// Since BTree stores {keyValues, count}, we need a probe object.
			// We only care about keyValues for matching.
			const probe = { keyValues: group.keyValues, count: 0 };
			const current = this.#groupCounts.get(probe);
			const currentCount = current?.count || 0;

			// Retract old state if it existed
			if (current) {
				outputChangeSet.append([{ ...current.keyValues, count: currentCount }, -1]);
			}

			// Calculate new state
			const newCount = group.rows.size;

			if (newCount > 0) {
				const newEntry = {
					keyValues: group.keyValues,
					count: newCount
				};
				// Update or create group count
				// Note: if current exists, BTree.set (via add) replaces it because comparator matches
				this.#groupCounts.add(newEntry);

				// Emit new state
				outputChangeSet.append([{ ...group.keyValues, count: newCount }, 1]);
			} else if (newCount === 0 && current) {
				// Remove group if count reaches zero
				// No need to emit +1, effectively deleted by the -1 retraction above
				this.#groupCounts.delete(current);
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			// clear any pushes we got before initial pull
			this.#groupCounts.clear();
			// Process initial data using pullRaw() for BTree consistency
			// only GroupBy has a pull raw
			for (const [group, weight] of (this.#source as any).pullRaw()) {
				this.#processGroup(group, weight);
			}

			this.#initialized = true;
		}

		for (const { keyValues, count } of this.#groupCounts.values()) {
			yield [{ ...keyValues, count }, 1] as [CountGroupResult<T>, number];
		}
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	/**
	 * Process a group with its weight (DBSP delta semantics)
	 * weight represents the change in row count for this group:
	 * - positive weight: rows were added to the group
	 * - negative weight: rows were removed from the group
	 */
	#processGroup(group: GroupedRow<T>, weight: number) {
		const probe = { keyValues: group.keyValues, count: 0 };
		const current = this.#groupCounts.get(probe);

		const newCount = group.rows.size;

		if (newCount > 0) {
			const newEntry = {
				keyValues: group.keyValues,
				count: newCount
			};
			this.#groupCounts.add(newEntry);
		} else if (newCount === 0 && current) {
			this.#groupCounts.delete(current);
		}
	}

	#createGroupResultComparator(): Comparator<{ keyValues: Partial<T>; count: number }> {
		return (a, b) => {
			const keyA = JSON.stringify(a.keyValues);
			const keyB = JSON.stringify(b.keyValues);
			if (keyA < keyB) return -1;
			if (keyA > keyB) return 1;
			return 0;
		};
	}
}
