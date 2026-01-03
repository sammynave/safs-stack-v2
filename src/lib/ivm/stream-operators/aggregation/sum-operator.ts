import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../../change-set/change-set.ts';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

interface SumOptions<T> {
	column: keyof T; // For COUNT(column) - only count non-null values
}

type SumResult = { sum: number };

/* stateful */
export class SumOperator<T> implements Sink<T>, Source<SumResult> {
	readonly #source: Source<T>;
	readonly #options: SumOptions<T>;
	#sink: Sink<SumResult> | typeof NullSink = NullSink;
	#sum: number = 0;
	#initialized = false;

	constructor(source: Source<T>, options: SumOptions<T>) {
		this.#source = source;
		this.#options = options;
		this.#source.setSink(this);
	}

	get size() {
		return 1; // Always returns a single count result
	}

	get sum() {
		return this.#sum;
	}

	setSink(sink: Sink<SumResult>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const oldSum = this.#sum;
		let sumChanged = false;
		for (const [row, weight] of changeSet.data) {
			if (this.#processRow(row, weight)) {
				sumChanged = true;
			}
		}

		if (sumChanged) {
			const outputChangeSet = new ChangeSet<SumResult>([
				[{ sum: oldSum }, -1],
				[{ sum: this.#sum }, 1]
			]);
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#sum = 0;
			// First call: initialize from source
			for (const [row, weight] of this.#source.pull()) {
				this.#processRow(row, weight);
			}
			this.#initialized = true;
		}

		yield [{ sum: this.#sum }, 1] as [SumResult, number];
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	#processRow(row: T, weight: number): boolean {
		const value = row[this.#options.column];

		if (value === null || value === undefined) {
			return false; // Don't count null values
		}

		// If column is specified, only count non-null values
		assert(
			typeof value === 'number',
			Error(`can only sum numbers - column: ${String(this.#options.column)} value: ${value}`)
		);

		if (weight === 0) return false;

		// Incremental update: add weight to count
		// +1 for insertions, -1 for deletions
		if (weight > 0) {
			this.#sum += value as number;
		} else if (weight < 0) {
			this.#sum -= value as number;
		}

		return true;
	}
}
