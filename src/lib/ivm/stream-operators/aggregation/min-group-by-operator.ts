import { ChangeSet } from '../../change-set/change-set.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import { BaseGroupByAggregation } from './base-group-by-aggregation.ts';
import { BTree } from '../../b-plus-tree/tree.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

type MinGroupResult<T> = Partial<T> & { min: number };

/* stateful */
export class MinGroupByOperator<T> implements Sink<GroupedRow<T>>, Source<MinGroupResult<T>> {
	#impl;

	constructor(source: Source<GroupedRow<T>>, column: keyof T) {
		source.setSink(this);
		this.#impl = new BaseGroupByAggregation(source, {
			resultKey: 'min',
			column,
			compute: (rows, col) => {
				let min: number | undefined;
				for (const row of rows.values()) {
					const value = row[col];
					if (value !== undefined) {
						if (min === undefined) {
							min = Number(value);
						} else {
							min = Math.min(min, Number(value));
						}
					}
				}
				return min;
			},
			shouldKeepGroup: (value, groupSize) => groupSize > 0 && value !== undefined
		});
	}

	get size() {
		return this.#impl.size;
	}

	setSink(sink: Sink<MinGroupResult<T>>) {
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
