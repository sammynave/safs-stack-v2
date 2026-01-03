import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../../change-set/change-set.ts';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

interface CountOptions<T> {
	column?: keyof T; // For COUNT(column) - only count non-null values
}

type CountResult = { count: number };

/* stateful */
export class CountOperator<T> implements Sink<T>, Source<CountResult> {
	readonly #source: Source<T>;
	readonly #options: CountOptions<T>;
	#sink: Sink<CountResult> | typeof NullSink = NullSink;
	#count: number = 0;
	#initialized = false;

	constructor(source: Source<T>, options: CountOptions<T> = {}) {
		this.#source = source;
		this.#options = options;
		this.#source.setSink(this);
	}

	get size() {
		return 1; // Always returns a single count result
	}

	get count() {
		return this.#count;
	}

	setSink(sink: Sink<CountResult>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const oldCount = this.#count;
		let countChanged = false;

		for (const [row, weight] of changeSet.data) {
			if (this.#processRow(row, weight)) {
				countChanged = true;
			}
		}

		// Only push if count actually changed
		if (countChanged) {
			const outputChangeSet = new ChangeSet<CountResult>([
				[{ count: oldCount }, -1],
				[{ count: this.#count }, 1]
			]);
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#count = 0;
			// Process initial data
			for (const [row, weight] of this.#source.pull()) {
				this.#processRow(row, weight);
			}
			this.#initialized = true;
		}

		yield [{ count: this.#count }, 1] as [CountResult, number];
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	/**
	 * Process a single row with its weight
	 * Returns true if count changed
	 */
	#processRow(row: T, weight: number): boolean {
		// If column is specified, only count non-null values
		if (this.#options.column !== undefined) {
			const value = row[this.#options.column];
			if (value === null || value === undefined) {
				return false; // Don't count null values
			}
		}

		// Incremental update: add weight to count
		// +1 for insertions, -1 for deletions
		this.#count += weight;
		return true;
	}
}
