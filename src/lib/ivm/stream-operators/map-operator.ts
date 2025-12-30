// src/lib/ivm/stream-operators/map-operator.ts
import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

type MapFn<T, U> = (row: T) => U;

/* stateless */
export class MapOperator<T, U> implements Sink<T>, Source<U> {
	readonly #source: Source<T>;
	readonly #mapFn: MapFn<T, U>;
	#sink: Sink<U> | typeof NullSink = NullSink;

	constructor(source: Source<T>, mapFn: MapFn<T, U>) {
		this.#source = source;
		this.#mapFn = mapFn;
		this.#source.setSink(this);
	}

	get size() {
		return this.#source.size;
	}

	setSink(sink: Sink<U>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		const mappedData = changeSet.data.map(([row, weight]) => {
			return [this.#mapFn(row), weight] as [U, number];
		});
		const nextChangeSet = new ChangeSet(mappedData);

		if (!nextChangeSet.isEmpty()) {
			this.#sink.push(nextChangeSet);
		}
	}

	*pull() {
		for (const [row, weight] of this.#source.pull()) {
			yield [this.#mapFn(row), weight] as [U, number];
		}
	}

	disconnect() {
		this.#source.disconnect(this);
	}
}
