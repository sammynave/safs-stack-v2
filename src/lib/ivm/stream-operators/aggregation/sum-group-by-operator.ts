import { ChangeSet } from '../../change-set/change-set.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import { BaseGroupByAggregation } from './base-group-by-aggregation.ts';
import { BTree } from '../../b-plus-tree/tree.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

type SumGroupResult<T> = Partial<T> & { sum: number };

/* stateful */
export class SumGroupByOperator<T> implements Sink<GroupedRow<T>>, Source<SumGroupResult<T>> {
	#impl;

	constructor(source: Source<GroupedRow<T>>, column: keyof T) {
		source.setSink(this);
		this.#impl = new BaseGroupByAggregation(source, {
			resultKey: 'sum',
			column,
			compute: (rows, col) => {
				return [...rows.values()].reduce((acc, v) => acc + (Number(v[col]) || 0), 0);
			},
			shouldKeepGroup: (value, groupSize) => groupSize > 0 // Keep groups only if they have rows
		});
	}

	get size() {
		return this.#impl.size;
	}

	setSink(sink: Sink<SumGroupResult<T>>) {
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
