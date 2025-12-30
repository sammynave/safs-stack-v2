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

export type AggregationConfig<T, TResult extends string> = {
	resultKey: TResult;
	column: keyof T;
	compute: (rows: BTree<T>, column: keyof T) => number | undefined;
	shouldKeepGroup: (value: number | undefined, groupSize: number) => boolean;
};

type GroupResult<T, TResult extends string> = Partial<T> & Record<TResult, number>;

export class BaseGroupByAggregation<T, TResult extends string> {
	readonly #source: Source<GroupedRow<T>>;
	readonly #config: AggregationConfig<T, TResult>;
	#sink: Sink<GroupResult<T, TResult>> | typeof NullSink = NullSink;
	#groupResults: BTree<{ keyValues: Partial<T>; value: number }>;
	#initialized = false;

	constructor(source: Source<GroupedRow<T>>, config: AggregationConfig<T, TResult>) {
		this.#source = source;
		this.#config = config;
		this.#groupResults = new BTree(this.#createGroupResultComparator());
	}

	get size() {
		return this.#groupResults.size;
	}

	setSink(newSink: Sink<GroupResult<T, TResult>>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = newSink;
	}

	push(changeSet: ChangeSet<GroupedRow<T>>) {
		type ResultType = GroupResult<T, TResult>;
		const outputChangeSet = new ChangeSet<ResultType>([]);

		for (const [group, weight] of changeSet.data) {
			if (weight === 0) continue;

			// Find existing entry in the BTree
			const probe = { keyValues: group.keyValues, value: 0 };
			const current = this.#groupResults.get(probe);
			const currentValue = current?.value ?? 0;

			const newValue = this.#config.compute(group.rows, this.#config.column);

			// Retract old state if it existed
			if (current) {
				outputChangeSet.append([
					{ ...current.keyValues, [this.#config.resultKey]: currentValue } as ResultType,
					-1
				]);
			}

			// Always update if group should be kept, regardless of weight
			if (this.#config.shouldKeepGroup(newValue, group.rows.size)) {
				const newEntry = { keyValues: group.keyValues, value: newValue ?? 0 };
				this.#groupResults.add(newEntry);
				outputChangeSet.append([
					{ ...group.keyValues, [this.#config.resultKey]: newValue ?? 0 } as ResultType,
					1
				]);
			} else if (current) {
				// Remove from storage if group should not be kept
				this.#groupResults.delete(current);
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		type ResultType = GroupResult<T, TResult>;

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

		for (const { keyValues, value } of this.#groupResults.values()) {
			yield [{ ...keyValues, [this.#config.resultKey]: value } as ResultType, 1] as [
				ResultType,
				number
			];
		}
	}

	disconnect() {
		this.#source.disconnect(this as any);
	}

	#processGroup(group: GroupedRow<T>, weight: number) {
		const probe = { keyValues: group.keyValues, value: 0 };
		const current = this.#groupResults.get(probe);
		const newValue = this.#config.compute(group.rows, this.#config.column);

		if (weight > 0 && this.#config.shouldKeepGroup(newValue, group.rows.size)) {
			const newEntry = {
				keyValues: group.keyValues,
				value: newValue ?? 0
			};
			this.#groupResults.add(newEntry);
		} else if (current) {
			// If group still has data and should be kept, update it; otherwise remove it
			if (this.#config.shouldKeepGroup(newValue, group.rows.size)) {
				const newEntry = { keyValues: group.keyValues, value: newValue ?? 0 };
				this.#groupResults.add(newEntry);
			} else {
				this.#groupResults.delete(current);
			}
		}
	}

	#createGroupResultComparator(): Comparator<{ keyValues: Partial<T>; value: number }> {
		return (a, b) => {
			const keyA = JSON.stringify(a.keyValues);
			const keyB = JSON.stringify(b.keyValues);
			if (keyA < keyB) return -1;
			if (keyA > keyB) return 1;
			return 0;
		};
	}
}
