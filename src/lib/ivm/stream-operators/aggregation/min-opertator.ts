import { assert } from '$lib/asserts.js';
import { BTree } from '$lib/ivm/b-plus-tree/tree.ts';
import { ChangeSet } from '../../change-set/change-set.ts';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

interface Options<T> {
	column: keyof T; // For COUNT(column) - only count non-null values
}

type Result = { min: number };

/* stateful */
export class MinOperator<T> implements Sink<T>, Source<Result> {
	readonly #source: Source<T>;
	readonly #options: Options<T>;
	#sink: Sink<Result> | typeof NullSink = NullSink;
	#records: BTree<T>;
	#initialized = false;

	constructor(source: Source<T>, options: Options<T>) {
		this.#source = source;
		this.#options = options;
		this.#source.setSink(this);
		// sort by column we care about, asc
		this.#records = new BTree<T>((a, b) => a[options.column] - b[options.column]);
	}

	get size() {
		return 1; // Always returns a single count result
	}

	get min() {
		return this.#minRecord?.[this.#options.column];
	}

	get #minRecord() {
		let first;
		for (const record of this.#records.values()) {
			first = record;
			break;
		}
		return first;
	}

	setSink(sink: Sink<Result>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const oldMin = this.min;
		let minChanged = false;
		for (const [row, weight] of changeSet.data) {
			if (this.#processRow(row, weight)) {
				minChanged = true;
			}
		}

		if (minChanged) {
			const outputChangeSet = new ChangeSet<Result>([
				[{ min: oldMin }, -1],
				[{ min: this.min }, 1]
			]);
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			this.#records.clear();
			// Process initial data
			for (const [row, weight] of this.#source.pull()) {
				this.#processRow(row, weight);
			}
			this.#initialized = true;
		}
		yield [{ min: this.min }, 1] as [Result, number];
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	#processRow(row: T, weight: number): boolean {
		if (weight === 0) return false;

		const value = row[this.#options.column];

		if (value === null || value === undefined) {
			return false; // Don't count null values
		}

		// If column is specified, only count non-null values
		assert(
			typeof value === 'number',
			Error(`can only 'min' numbers - column: ${String(this.#options.column)} value: ${value}`)
		);
		if (weight > 0) {
			this.#records.add(row);
		} else {
			this.#records.delete(row);
		}

		return true;
	}
}
