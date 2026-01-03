import { assert } from '$lib/asserts.js';
import { BTree } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';
import type { Comparator } from '../b-plus-tree/tree.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};
type MaterializedGroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: T[]; // Array instead of BTree
};

/* stateful */
export class GroupByOperator<T> implements Sink<T>, Source<MaterializedGroupedRow<T>> {
	readonly #source: Source<T>;
	readonly #keys: (keyof T)[];
	readonly #groups: BTree<GroupedRow<T>>;
	readonly #rowComparator: Comparator<T>;
	#sink: Sink<GroupedRow<T>> | typeof NullSink = NullSink;
	#initialized = false;

	constructor(source: Source<T>, keys: (keyof T)[], rowComparator: Comparator<T>) {
		this.#source = source;
		this.#keys = keys;
		this.#rowComparator = rowComparator;
		this.#source.setSink(this);
		this.#groups = new BTree(this.#createGroupComparator());
	}

	get size() {
		return this.#groups.size;
	}

	setSink(sink: Sink<MaterializedGroupedRow<T>>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const affectedGroups = new Map<
			string,
			{
				keyValues: Partial<T>;
				oldRowCount: number;
			}
		>();
		const outputChangeSet = new ChangeSet<GroupedRow<T>>([]);

		for (const [row, weight] of changeSet.data) {
			const keyValues = this.#extractKeyValues(row);
			const compositeKey = this.#createCompositeKey(keyValues);
			this.#initializeAffectedGroup(affectedGroups, compositeKey, keyValues);
			this.#applyWeightToGroup(row, weight);
		}

		for (const [_compositeKey, delta] of affectedGroups) {
			this.#buildOutputForGroup(delta, outputChangeSet);
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#groups.clear();
			// Process initial data
			for (const [row] of this.#source.pull()) {
				this.#addRowToGroup(row);
			}
			this.#initialized = true;
		}
		for (const group of this.#groups.values()) {
			yield [
				{
					keys: group.keys,
					keyValues: group.keyValues,
					rows: Array.from(group.rows.values())
				},
				1
			] satisfies [MaterializedGroupedRow<T>, number];
		}
	}

	// For operators like HAVING that need BTree
	*pullRaw() {
		if (!this.#initialized) {
			this.#groups.clear();
			// Process initial data
			for (const [row] of this.#source.pull()) {
				this.#addRowToGroup(row);
			}
			this.#initialized = true;
		}
		for (const group of this.#groups.values()) {
			yield [group, 1];
		}
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	#addRowToGroup(row: T) {
		const keyValues = this.#extractKeyValues(row);
		const existingGroup = this.#findGroup(keyValues);

		if (existingGroup) {
			// Group exists: delete it, add row to inner BTree, re-add to outer BTree
			this.#groups.delete(existingGroup);
			existingGroup.rows.add(row);
			this.#groups.add(existingGroup);
		} else {
			// Group doesn't exist: create new group with inner BTree
			const newRowsBTree = new BTree(this.#rowComparator);
			newRowsBTree.add(row);
			const newGroup: GroupedRow<T> = {
				keys: this.#keys,
				keyValues,
				rows: newRowsBTree
			};
			this.#groups.add(newGroup);
		}
	}

	#removeRowFromGroup(row: T) {
		const keyValues = this.#extractKeyValues(row);
		const existingGroup = this.#findGroup(keyValues);

		if (existingGroup) {
			// Delete the group, remove row from inner BTree, re-add if not empty
			this.#groups.delete(existingGroup);
			existingGroup.rows.delete(row);

			if (existingGroup.rows.size > 0) {
				this.#groups.add(existingGroup);
			}
			// If size is 0, we don't re-add the group (it's been fully deleted)
		}
	}

	#findGroup(keyValues: Partial<T>): GroupedRow<T> | undefined {
		// Use BTree.get() which uses the comparator for O(log N) lookup
		// instead of iterating through all groups
		const probe: GroupedRow<T> = {
			keys: this.#keys,
			keyValues,
			rows: null as any // Comparator only checks keyValues
		};
		return this.#groups.get(probe);
	}

	#extractKeyValues(row: T): Partial<T> {
		const keyValues: Partial<T> = {};
		for (const key of this.#keys) {
			keyValues[key] = row[key];
		}
		return keyValues;
	}

	#createCompositeKey(keyValues: Partial<T>): string {
		const groupKeyArr: string[] = [];
		this.#keys.forEach((key) => {
			groupKeyArr.push(`${keyValues[key]}`);
		});
		return groupKeyArr.join(':');
	}

	#createGroupComparator(): Comparator<GroupedRow<T>> {
		return (a, b) => {
			for (const key of this.#keys) {
				const valA = a.keyValues[key];
				const valB = b.keyValues[key];
				if (valA === valB) continue;
				if (valA === undefined || valA === null) return -1;
				if (valB === undefined || valB === null) return 1;
				if (valA < valB) return -1;
				if (valA > valB) return 1;
			}
			return 0;
		};
	}

	#initializeAffectedGroup(
		affectedGroups: Map<string, { keyValues: Partial<T>; oldRowCount: number }>,
		compositeKey: string,
		keyValues: Partial<T>
	) {
		if (affectedGroups.has(compositeKey)) return;

		const existingGroup = this.#findGroup(keyValues);
		affectedGroups.set(compositeKey, {
			keyValues,
			oldRowCount: existingGroup ? existingGroup.rows.size : 0
		});
	}

	#applyWeightToGroup(row: T, weight: number) {
		if (weight === 0) return;

		const absWeight = Math.abs(weight);
		const operation = weight > 0 ? this.#addRowToGroup : this.#removeRowFromGroup;

		for (let i = 0; i < absWeight; i++) {
			operation.call(this, row);
		}
	}
	#buildOutputForGroup(delta, outputChangeSet) {
		const group = this.#findGroup(delta.keyValues);

		// Always retract old state if it existed
		if (delta.oldRowCount > 0) {
			const oldGroup: GroupedRow<T> = {
				keys: this.#keys,
				keyValues: delta.keyValues,
				rows: new BTree(this.#rowComparator) // Empty, just for structure
			};
			outputChangeSet.append([oldGroup, -1]);
		}

		// Always emit new state if group exists
		if (group && group.rows.size > 0) {
			outputChangeSet.append([group, 1]);
		}
	}
	// TODO compare to new fn above to make sure we aren't missing any cases
	#buildOutputForGroupOld(
		delta: { keyValues: Partial<T>; oldRowCount: number },
		outputChangeSet: ChangeSet<GroupedRow<T>>
	) {
		const group = this.#findGroup(delta.keyValues);
		const newRowCount = group ? group.rows.size : 0;
		const rowCountDelta = newRowCount - delta.oldRowCount;

		if (rowCountDelta === 0) return;

		if (group) {
			outputChangeSet.append([group, 1]);
			return;
		}

		// Group was deleted entirely - emit empty group for deletion
		if (delta.oldRowCount > 0) {
			const emptyGroup: GroupedRow<T> = {
				keys: this.#keys,
				keyValues: delta.keyValues,
				rows: new BTree(this.#rowComparator)
			};
			outputChangeSet.append([emptyGroup, -1]);
		}
	}
}
