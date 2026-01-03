import { assert } from '$lib/asserts.js';
import { ChangeSet } from '$lib/ivm/change-set/change-set.ts';
import { NullSink } from '$lib/ivm/sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import type { JSONValue } from './json-agg-group-by-operator.ts';

interface JsonAggOptions<T> {
	column: keyof T; // Column to aggregate into array
}

type JsonAggResult = { jsonAgg: JSONValue[] };

/* stateful */
export class JsonAggOperator<T> implements Sink<T>, Source<JsonAggResult> {
	readonly #source: Source<T>;
	readonly #options: JsonAggOptions<T>;
	#sink: Sink<JsonAggResult> | typeof NullSink = NullSink;
	#array: JSONValue[] = [];
	#initialized = false;

	constructor(source: Source<T>, options: JsonAggOptions<T>) {
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

	setSink(sink: Sink<JsonAggResult>) {
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
			const outputChangeSet = new ChangeSet<JsonAggResult>([
				[{ jsonAgg: oldArray }, -1],
				[{ jsonAgg: [...this.#array] }, 1]
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

		yield [{ jsonAgg: [...this.#array] }, 1] as [JsonAggResult, number];
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	#processRow(row: T, weight: number): boolean {
		const value = row[this.#options.column];

		if (value === null || value === undefined) {
			return false; // Don't aggregate null values
		}

		if (weight === 0) return false;

		// Incremental update: add or remove from array
		// +1 for insertions, -1 for deletions
		if (weight > 0) {
			this.#array.push(value as JSONValue);
		} else if (weight < 0) {
			// Remove first occurrence of the value
			// For objects, we need to find by deep equality
			const index = this.#array.findIndex((item) => {
				if (typeof item === 'object' && typeof value === 'object') {
					return JSON.stringify(item) === JSON.stringify(value);
				}
				return item === value;
			});
			if (index !== -1) {
				this.#array.splice(index, 1);
			}
		}

		return true;
	}
}
