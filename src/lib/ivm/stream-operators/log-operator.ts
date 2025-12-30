import { ChangeSet } from '../change-set/change-set.ts';
import type { Sink, Source } from './stream-operator-types.ts';
import { NullSink } from '../sinks/null-sink.ts';
import { assert } from '$lib/asserts.ts';

/* stateless */
export class LogSink<T> implements Sink<T> {
	readonly #source: Source<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;

	constructor(source: Source<T>) {
		this.#source = source;
		this.#source.setSink(this);
	}

	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>): void {
		console.log('LOG SINK PUSH:', changeSet.data);
		this.#sink.push(changeSet);
	}

	*pull() {
		for (const [row, weight] of this.#source.pull()) {
			console.log('LOG SINK PULL:', [row, weight]);
			yield [row, weight] as [T, number];
		}
	}
}
