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

type ArrayAggGroupResult<T> = Partial<T> & { arrayAgg: string[] };

/* stateful */
export class ArrayAggGroupByOperator<T>
	implements Sink<GroupedRow<T>>, Source<ArrayAggGroupResult<T>>
{
	readonly #source: Source<GroupedRow<T>>;
	readonly #column: keyof T;
	#sink: Sink<ArrayAggGroupResult<T>> | typeof NullSink = NullSink;
	#groupResults: BTree<{ keyValues: Partial<T>; array: string[] }>;
	#initialized = false;

	constructor(source: Source<GroupedRow<T>>, column: keyof T) {
		this.#source = source;
		this.#column = column;
		this.#groupResults = new BTree(this.#createGroupResultComparator());
		this.#source.setSink(this);
	}

	get size() {
		return this.#groupResults.size;
	}

	setSink(newSink: Sink<ArrayAggGroupResult<T>>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = newSink;
	}

	push(changeSet: ChangeSet<GroupedRow<T>>) {
		const outputChangeSet = new ChangeSet<ArrayAggGroupResult<T>>([]);

		for (const [group, weight] of changeSet.data) {
			if (weight === 0) continue;

			// Find existing entry in the BTree
			const probe = { keyValues: group.keyValues, array: [] };
			const current = this.#groupResults.get(probe);

			// Retract old state if it existed
			if (current) {
				outputChangeSet.append([
					{ ...current.keyValues, arrayAgg: [...current.array] } as ArrayAggGroupResult<T>,
					-1
				]);
			}

			// Compute new array for this group
			const newArray = this.#computeArray(group.rows);

			// Always update if group has rows
			if (group.rows.size > 0) {
				const newEntry = { keyValues: group.keyValues, array: newArray };
				this.#groupResults.add(newEntry);
				outputChangeSet.append([
					{ ...group.keyValues, arrayAgg: [...newArray] } as ArrayAggGroupResult<T>,
					1
				]);
			} else if (current) {
				// Remove from storage if group is now empty
				this.#groupResults.delete(current);
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			// clear any pushes we got before initial pull
			this.#groupResults.clear();
			this.#initialized = true;
		}

		// Always re-process from source to get current state
		this.#groupResults.clear();
		for (const [group, weight] of (this.#source as any).pullRaw()) {
			this.#processGroup(group, weight);
		}

		for (const { keyValues, array } of this.#groupResults.values()) {
			yield [{ ...keyValues, arrayAgg: [...array] } as ArrayAggGroupResult<T>, 1] as [
				ArrayAggGroupResult<T>,
				number
			];
		}
	}

	disconnect() {
		this.#source.disconnect(this as any);
	}

	#processGroup(group: GroupedRow<T>, weight: number) {
		const probe = { keyValues: group.keyValues, array: [] };
		const current = this.#groupResults.get(probe);
		const newArray = this.#computeArray(group.rows);

		if (weight > 0 && group.rows.size > 0) {
			const newEntry = {
				keyValues: group.keyValues,
				array: newArray
			};
			this.#groupResults.add(newEntry);
		} else if (current) {
			// If group still has data, update it; otherwise remove it
			if (group.rows.size > 0) {
				const newEntry = { keyValues: group.keyValues, array: newArray };
				this.#groupResults.add(newEntry);
			} else {
				this.#groupResults.delete(current);
			}
		}
	}

	#computeArray(rows: BTree<T>): string[] {
		const result: string[] = [];
		for (const row of rows.values()) {
			const value = row[this.#column];
			// Only include non-null string values
			if (value !== null && value !== undefined && typeof value === 'string') {
				result.push(value);
			}
		}
		return result;
	}

	#createGroupResultComparator(): Comparator<{ keyValues: Partial<T>; array: string[] }> {
		return (a, b) => {
			const keyA = JSON.stringify(a.keyValues);
			const keyB = JSON.stringify(b.keyValues);
			if (keyA < keyB) return -1;
			if (keyA > keyB) return 1;
			return 0;
		};
	}
}
