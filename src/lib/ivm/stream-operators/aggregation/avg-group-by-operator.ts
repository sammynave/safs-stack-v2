import { ChangeSet } from '../../change-set/change-set.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import { BaseGroupByAggregation } from './base-group-by-aggregation.ts';
import { BTree } from '../../b-plus-tree/tree.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

type AvgGroupResult<T> = Partial<T> & { avg: number };

/* stateful */
export class AvgGroupByOperator<T> implements Sink<GroupedRow<T>>, Source<AvgGroupResult<T>> {
	#impl;

	constructor(source: Source<GroupedRow<T>>, column: keyof T) {
		source.setSink(this);
		this.#impl = new BaseGroupByAggregation(source, {
			resultKey: 'avg',
			column,
			compute: (rows, col) => {
				let sum = 0;
				let count = 0;
				for (const row of rows.values()) {
					const value = row[col];
					if (value !== undefined && value !== null) {
						sum += Number(value);
						count++;
					}
				}
				return count > 0 ? sum / count : 0;
			},
			shouldKeepGroup: (value, groupSize) => groupSize > 0 && value !== undefined
		});
	}

	get size() {
		return this.#impl.size;
	}

	setSink(sink: Sink<AvgGroupResult<T>>) {
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
