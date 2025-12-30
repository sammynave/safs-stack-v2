import { assert } from '$lib/asserts.js';
import { ChangeSet } from '$lib/ivm/change-set/change-set.ts';
import { NullSink } from '$lib/ivm/sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

interface ArrayAggOptions<T> {
	column: keyof T; // Column to aggregate into array
}

type ArrayAggResult = { arrayAgg: string[] };

/* stateful */
export class ArrayAggOperator<T> implements Sink<T>, Source<ArrayAggResult> {
	readonly #source: Source<T>;
	readonly #options: ArrayAggOptions<T>;
	#sink: Sink<ArrayAggResult> | typeof NullSink = NullSink;
	#array: string[] = [];
	#initialized = false;

	constructor(source: Source<T>, options: ArrayAggOptions<T>) {
		this.#source = source;
		this.#options = options;
		this.#source.setSink(this);
	}

	get size() {
		return 1; // Always returns a single array result
	}

	get array() {
		return this.#array;
	}

	setSink(sink: Sink<ArrayAggResult>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const oldArray = [...this.#array];
		let arrayChanged = false;
		for (const [row, weight] of changeSet.data) {
			if (this.#processRow(row, weight)) {
				arrayChanged = true;
			}
		}

		if (arrayChanged) {
			const outputChangeSet = new ChangeSet<ArrayAggResult>([
				[{ arrayAgg: oldArray }, -1],
				[{ arrayAgg: [...this.#array] }, 1]
			]);
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#array = [];
			// First call: initialize from source
			for (const [row, weight] of this.#source.pull()) {
				this.#processRow(row, weight);
			}
			this.#initialized = true;
		}

		yield [{ arrayAgg: [...this.#array] }, 1] as [ArrayAggResult, number];
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	#processRow(row: T, weight: number): boolean {
		const value = row[this.#options.column];

		if (value === null || value === undefined) {
			return false; // Don't aggregate null values
		}

		// Only aggregate string values
		assert(
			typeof value === 'string',
			Error(`can only array agg strings - column: ${String(this.#options.column)} value: ${value}`)
		);

		if (weight === 0) return false;

		// Incremental update: add or remove from array
		// +1 for insertions, -1 for deletions
		if (weight > 0) {
			this.#array.push(value as string);
		} else if (weight < 0) {
			// Remove first occurrence of the value
			const index = this.#array.indexOf(value as string);
			if (index !== -1) {
				this.#array.splice(index, 1);
			}
		}

		return true;
	}
}
