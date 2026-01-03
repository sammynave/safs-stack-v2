import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/**
 * MultiRowCombineOperator merges multiple rows from two sources by matching key columns.
 * Used for combining multiple GROUP BY aggregations (e.g., count + sum + avg per group).
 *
 * Unlike CombineOperator which handles single values, this handles multiple rows
 * and merges them based on matching key columns.
 */
export class MultiRowCombineOperator<T, U, R> implements Source<R> {
	readonly #leftSource: Source<T>;
	readonly #rightSource: Source<U>;
	readonly #merger: (left: T, right: U) => R;
	#sink: Sink<R> | typeof NullSink = NullSink;

	#leftRows: Map<string, T> = new Map();
	#rightRows: Map<string, U> = new Map();
	#currentResults: Map<string, R> = new Map();
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
		// Size is determined by pull()
		return 0;
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
			this.#initializeValues();
			this.#initialized = true;
		}

		// Yield all current results
		for (const result of this.#currentResults.values()) {
			yield [result, 1] as [R, number];
		}
	}

	#initializeValues() {
		// Pull initial values from both sources and store in maps
		for (const [value, weight] of this.#leftSource.pull()) {
			if (weight > 0) {
				const key = this.#extractKey(value);
				this.#leftRows.set(key, value);
			}
		}

		for (const [value, weight] of this.#rightSource.pull()) {
			if (weight > 0) {
				const key = this.#extractKey(value);
				this.#rightRows.set(key, value);
			}
		}

		// Compute initial merged results
		for (const [key, leftRow] of this.#leftRows) {
			const rightRow = this.#rightRows.get(key);
			if (rightRow) {
				const merged = this.#merger(leftRow, rightRow);
				this.#currentResults.set(key, merged);
			}
		}
	}

	disconnect() {
		this.#leftSource.disconnect(this.#leftSink);
		this.#rightSource.disconnect(this.#rightSink);
	}

	#handleLeftPush(changeSet: ChangeSet<T>) {
		// Update left rows map based on changeSet
		for (const [value, weight] of changeSet.data) {
			const key = this.#extractKey(value);
			if (weight > 0) {
				this.#leftRows.set(key, value);
			} else if (weight < 0) {
				this.#leftRows.delete(key);
			}
		}
		this.#recompute();
	}

	#handleRightPush(changeSet: ChangeSet<U>) {
		// Update right rows map based on changeSet
		for (const [value, weight] of changeSet.data) {
			const key = this.#extractKey(value);
			if (weight > 0) {
				this.#rightRows.set(key, value);
			} else if (weight < 0) {
				this.#rightRows.delete(key);
			}
		}
		this.#recompute();
	}

	#recompute() {
		const changes: [R, number][] = [];
		const allKeys = new Set([...this.#leftRows.keys(), ...this.#rightRows.keys()]);

		for (const key of allKeys) {
			const oldResult = this.#currentResults.get(key);
			const leftRow = this.#leftRows.get(key);
			const rightRow = this.#rightRows.get(key);

			// Compute new result (only if both rows exist)
			const newResult = leftRow && rightRow ? this.#merger(leftRow, rightRow) : undefined;

			// Compare and emit changes
			if (!this.#resultsEqual(oldResult, newResult)) {
				if (oldResult !== undefined) {
					changes.push([oldResult, -1]);
				}
				if (newResult !== undefined) {
					changes.push([newResult, 1]);
					this.#currentResults.set(key, newResult);
				} else {
					this.#currentResults.delete(key);
				}
			}
		}

		if (changes.length > 0) {
			this.#sink.push(new ChangeSet(changes));
		}
	}

	#resultsEqual(a: R | undefined, b: R | undefined): boolean {
		if (a === undefined || b === undefined) {
			return a === b;
		}
		return JSON.stringify(a) === JSON.stringify(b);
	}

	#extractKey(row: any): string {
		// Extract the key columns (everything except the aggregation result)
		// For GROUP BY results, the key columns are the non-aggregation fields
		const keyObj: any = {};
		for (const [key, value] of Object.entries(row)) {
			// Skip known aggregation result keys
			if (!['count', 'sum', 'avg', 'min', 'max'].includes(key)) {
				keyObj[key] = value;
			}
		}
		return JSON.stringify(keyObj);
	}
}
