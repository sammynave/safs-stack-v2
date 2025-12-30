import { ChangeSet } from '../../change-set/change-set.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import { BaseGroupByAggregation } from './base-group-by-aggregation.ts';
import { BTree } from '../../b-plus-tree/tree.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

type MaxGroupResult<T> = Partial<T> & { max: number };

/* stateful */
export class MaxGroupByOperator<T> implements Sink<GroupedRow<T>>, Source<MaxGroupResult<T>> {
	#impl;

	constructor(source: Source<GroupedRow<T>>, column: keyof T) {
		source.setSink(this);
		this.#impl = new BaseGroupByAggregation(source, {
			resultKey: 'max',
			column,
			compute: (rows, col) => {
				let max: number | undefined;
				for (const row of rows.values()) {
					const value = row[col];
					if (value !== undefined) {
						if (max === undefined) {
							max = Number(value);
						} else {
							max = Math.max(max, Number(value));
						}
					}
				}
				return max;
			},
			shouldKeepGroup: (value, groupSize) => groupSize > 0 && value !== undefined
		});
	}

	get size() {
		return this.#impl.size;
	}

	setSink(sink: Sink<MaxGroupResult<T>>) {
		return this.#impl.setSink(sink);
	}

	push(changeSet: ChangeSet<GroupedRow<T>>) {
		return this.#impl.push(changeSet);
	}

	*pull() {
		yield* this.#impl.pull();
	}

	disconnect() {
		return this.#impl.disconnect();
	}
}
