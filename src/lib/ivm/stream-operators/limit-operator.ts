import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSetAlgebra } from '../change-set/change-set-algebra.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/* stateful */
export class LimitOperator<T> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	#limit: number;
	#sink: Sink<T> | typeof NullSink = NullSink;
	#topK: BTree<T>;
	#lastOutput: ChangeSet<T>;
	#hasInitialized: boolean = false;

	// Can we search up and down the chain for a sort operation?
	// maybe you always have to pass a sort to `Memory` source?
	constructor(
		source: Source<T>,
		limit: number = Number.MAX_SAFE_INTEGER,
		comparator: Comparator<T>
	) {
		this.#source = source;
		this.#limit = limit;
		this.#topK = new BTree<T>(comparator);
		this.#lastOutput = ChangeSetAlgebra.zero();
		this.#source.setSink(this);
	}
	get limit() {
		return this.#limit;
	}
	get size() {
		return this.#topK.size;
	}
	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const outputDelta = this.processIncrement(changeSet);
		this.#sink.push(outputDelta);
		if (this.#limit < this.size) {
			throw Error(
				`Size > limit somehow: ${JSON.stringify(this.currentState, null, 2)} -- changeset: ${changeSet} -- outputdelta: ${outputDelta}`
			);
		}
	}

	*pull() {
		let count = 0;
		for (const [row, weight] of this.#source.pull()) {
			if (weight > 0) {
				count++;
				this.#handleAddition(row);

				// Only update #lastOutput on first pull
				if (!this.#hasInitialized) {
					this.#lastOutput = ChangeSetAlgebra.add(this.#lastOutput, new ChangeSet([[row, 1]]));
				}
				yield [row, 1] as [T, number];

				if (count >= this.#limit) {
					break;
				}
			}
		}
		this.#hasInitialized = true;
	}

	/**
	 * Compute output delta by comparing current state with last output
	 */
	#computeOutputDelta(): ChangeSet<T> {
		const delta = ChangeSetAlgebra.subtract(this.currentState, this.#lastOutput);
		this.#lastOutput = this.currentState;
		return delta;
	}

	disconnect() {
		this.#topK.clear();
		this.#lastOutput = ChangeSetAlgebra.zero();
		this.#source.disconnect(this);
	}

	processIncrement(delta: ChangeSet<T>): ChangeSet<T> {
		let needsRefill = false;

		// Merge delta to handle multiple updates to same row
		const mergedDelta = delta.mergeRecords();

		// Process each change
		for (const [row, weight] of mergedDelta.data) {
			if (weight > 0) {
				// Addition or increase weight
				this.#handleAddition(row);
			} else {
				// Removal or decrease weight
				this.#handleRemoval(row);
			}
		}
		// Refill vacancies if needed
		if (this.size < this.#limit) {
			this.#refillToLimit();
		}

		// Compute and return output delta
		return this.#computeOutputDelta();
	}

	get currentState(): ChangeSet<T> {
		const state = [];
		for (const row of this.#topK.values()) {
			state.push([row, 1] as [T, number]);
		}
		return new ChangeSet(state);
	}

	#handleAddition(row: T): void {
		// If we're below limit, just add
		if (this.#topK.size < this.#limit) {
			this.#topK.add(row);
			return;
		}

		// At capacity - compare with worst before adding
		const worst = this.#getWorstElement();

		if (this.#limit === this.#topK.size) {
			// Only add if the new row is better than the worst
			// Comparator returns negative if row < worst (row is better)
			if (this.#topK.comparator(row, worst) < 0) {
				const didDelete = this.#topK.delete(worst);
				if (!didDelete) {
					throw Error(`Could not find entry to delete ${worst}`);
				}
				this.#topK.add(row);
			}
		}
		// Otherwise, row is worse than worst - don't add it
	}

	#handleRemoval(row: T): boolean {
		return this.#topK.delete(row);
	}

	/**
	 * Refill top-k by pulling more data from source until we reach limit
	 */
	#refillToLimit(): void {
		for (const [row, weight] of this.#source.pull()) {
			// Only consider positive weights and rows not already in top-k
			if (weight > 0 && !this.#topK.has(row)) {
				this.#topK.add(row);

				// Early exit once we've filled to the limit
				if (this.#topK.size >= this.#limit) {
					break;
				}
			}
		}
	}

	/**
	 * Get the worst (highest by comparator) element in top-k
	 */
	#getWorstElement(): T | undefined {
		// The worst element is the last one when iterating in reverse
		let worst;
		for (const w of this.#topK.valuesReversed()) {
			worst = w;
			this.work++;
			break;
		}
		return worst;
	}
}
