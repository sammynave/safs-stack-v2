import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/**
 * MergeOperator takes two sources and merges their outputs using a merger function.
 * Emits a new result whenever either source changes.
 *
 * Usage:
 *   const merged = new CombineOperator(
 *     sumOperator,
 *     countOperator,
 *     ({sum}, {count}) => ({avg: count > 0 ? sum / count : null})
 *   );
 */
export class CombineOperator<T, U, R> implements Source<R> {
	readonly #leftSource: Source<T>;
	readonly #rightSource: Source<U>;
	readonly #merger: (left: T, right: U) => R;
	#sink: Sink<R> | typeof NullSink = NullSink;

	#leftValue: T | undefined;
	#rightValue: U | undefined;
	#currentResult: R | undefined;
	#initialized = false;

	readonly #leftSink: Sink<T>;
	readonly #rightSink: Sink<U>;

	constructor(leftSource: Source<T>, rightSource: Source<U>, merger: (left: T, right: U) => R) {
		this.#leftSource = leftSource;
		this.#rightSource = rightSource;
		this.#merger = merger;

		// Create internal sinks that forward to our handlers
		this.#leftSink = {
			push: (changeSet: ChangeSet<T>) => this.#handleLeftPush(changeSet)
		};
		this.#rightSink = {
			push: (changeSet: ChangeSet<U>) => this.#handleRightPush(changeSet)
		};

		// Connect sources to our internal sinks
		this.#leftSource.setSink(this.#leftSink);
		this.#rightSource.setSink(this.#rightSink);
	}

	get size() {
		return this.#currentResult !== undefined ? 1 : 0;
	}

	setSink(sink: Sink<R>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	*pull() {
		if (!this.#initialized) {
			this.#currentResult = undefined;
			this.#initializeValues();
			this.#initialized = true;
		}

		// Initialize with current values from both sources
		// this.#initializeValues();

		yield [this.#currentResult, 1] as [R, number];
	}

	disconnect() {
		this.#leftSource.disconnect(this.#leftSink);
		this.#rightSource.disconnect(this.#rightSink);
	}

	#initializeValues() {
		// Pull initial values from both sources
		for (const [value, weight] of this.#leftSource.pull()) {
			if (weight > 0) {
				this.#leftValue = value;
			}
		}
		for (const [value, weight] of this.#rightSource.pull()) {
			if (weight > 0) {
				this.#rightValue = value;
			}
		}

		// Compute initial result if both values are available
		if (this.#leftValue !== undefined && this.#rightValue !== undefined) {
			this.#currentResult = this.#merger(this.#leftValue, this.#rightValue);
		}
	}

	#handleLeftPush(changeSet: ChangeSet<T>) {
		// Update left value with the latest from changeSet
		for (const [value, weight] of changeSet.data) {
			if (weight > 0) {
				this.#leftValue = value;
			}
		}
		this.#recompute();
	}

	#handleRightPush(changeSet: ChangeSet<U>) {
		// Update right value with the latest from changeSet
		for (const [value, weight] of changeSet.data) {
			if (weight > 0) {
				this.#rightValue = value;
			}
		}
		this.#recompute();
	}

	#recompute() {
		if (this.#leftValue === undefined || this.#rightValue === undefined) {
			return; // Can't compute without both values
		}

		const oldResult = this.#currentResult;
		const newResult = this.#merger(this.#leftValue, this.#rightValue);

		// Only emit if result changed
		if (!this.#resultsEqual(oldResult, newResult)) {
			this.#currentResult = newResult;

			const changes: [R, number][] = [];
			if (oldResult !== undefined) {
				changes.push([oldResult, -1]);
			}
			changes.push([newResult, 1]);

			this.#sink.push(new ChangeSet(changes));
		}
	}

	#resultsEqual(a: R | undefined, b: R | undefined): boolean {
		if (a === undefined || b === undefined) {
			return a === b;
		}
		// Deep equality check for objects
		return JSON.stringify(a) === JSON.stringify(b);
	}
}
