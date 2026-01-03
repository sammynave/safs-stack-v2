import { assert } from '$lib/asserts.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

type Predicate<T> = (row: T) => boolean;

/* stateless */
export class FilterOperator<T> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	readonly #predicate: Predicate<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;

	constructor(source: Source<T>, predicate: Predicate<T>) {
		this.#source = source;
		this.#predicate = predicate;
		this.#source.setSink(this);
	}

	get size() {
		return this.#source.size;
	}
	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const nextChangeSet = this.#filterChangeSet(changeSet);
		if (!nextChangeSet.isEmpty()) {
			this.#sink.push(nextChangeSet);
		}
	}

	*pull() {
		for (const [row, weight] of this.#source.pull()) {
			if (this.#predicate(row)) {
				yield [row, weight] as [T, number];
			}
		}
	}

	#filterChangeSet(zset: ChangeSet<T>): ChangeSet<T> {
		const filteredData = zset.data.filter(([record]) => this.#predicate(record));
		return new ChangeSet(filteredData);
	}

	disconnect() {
		this.#source.disconnect(this);
	}
}
