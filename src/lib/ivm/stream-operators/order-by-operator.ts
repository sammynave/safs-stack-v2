import { assert } from '$lib/asserts.ts';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import type { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

// Maybe more operators can be stateless?
/* stateful */
export class OrderByOperator<T> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	readonly #comparator: Comparator<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;
	#sortedData: BTree<T>;
	#initialized = false;

	constructor(source: Source<T>, comparator: Comparator<T>) {
		this.#source = source;
		this.#comparator = comparator;
		this.#sortedData = new BTree(comparator);
		this.#source.setSink(this);
	}

	get size() {
		return this.#sortedData.size;
	}
	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		// Update sorted BTree
		for (const [row, weight] of changeSet.data) {
			if (weight > 0) {
				this.#sortedData.add(row);
			} else {
				this.#sortedData.delete(row);
			}
		}
		// Forward the changeSet (still sorted)
		this.#sink.push(changeSet);
	}

	*pull() {
		if (!this.#initialized) {
			this.#sortedData.clear();
			for (const [row, weight] of this.#source.pull()) {
				if (weight > 0) {
					this.#sortedData.add(row);
				}
			}
			this.#initialized = true;
		}

		// Yield in sorted order
		for (const row of this.#sortedData.values()) {
			yield [row, 1] as [T, number];
		}
	}
	disconnect() {
		this.#sortedData.clear();
		this.#source.disconnect(this);
	}
}
