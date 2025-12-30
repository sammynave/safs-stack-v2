import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

/**
 * SplitStreamOperator splits a single source stream into multiple independent output streams.
 * Each sink receives the same changeSet.
 *
 * Usage:
 *   const split = new SplitStreamOperator(source);
 *   const sink1 = split.branch(); // Creates a branch that can be used as a Source
 *   const sink2 = split.branch();
 */
export class SplitStreamOperator<T> implements Sink<T>, Source<T> {
	readonly #source: Source<T>;
	readonly #branches: Branch<T>[] = [];

	constructor(source: Source<T>) {
		this.#source = source;
		this.#source.setSink(this);
	}

	get size() {
		throw Error('Can not call size directly. User a branch');
		return 0;
	}

	setSink(sink: Sink<T>) {
		throw Error('Can not set sink directly. Use a Branch.', { cause: sink });
	}

	/**
	 * Creates a new branch that acts as an independent Source<T>
	 */
	branch(): Source<T> {
		const branch = new Branch<T>(this.#source);
		this.#branches.push(branch);
		return branch;
	}

	push(changeSet: ChangeSet<T>) {
		// Forward the changeSet to all branches
		for (const branch of this.#branches) {
			branch.push(changeSet);
		}
	}

	*pull() {
		throw Error('Can not pull directly. Use a Branch.');
	}

	disconnect() {
		this.#source.disconnect(this);
		for (const branch of this.#branches) {
			branch.disconnect();
		}
	}
}

/**
 * Branch acts as an independent Source that receives data from SplitStreamOperator
 */
class Branch<T> implements Source<T> {
	readonly #upstreamSource: Source<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;

	constructor(upstreamSource: Source<T>) {
		this.#upstreamSource = upstreamSource;
	}

	get size() {
		return this.#upstreamSource.size;
	}

	setSink(sink: Sink<T>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed per branch`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<T>) {
		this.#sink.push(changeSet);
	}

	*pull() {
		yield* this.#upstreamSource.pull();
	}

	*pullRaw() {
		yield* this.#upstreamSource.pullRaw();
	}

	disconnect(sink?: Sink<T>) {
		if (sink === undefined || sink === this.#sink) {
			this.#sink = NullSink;
		}
	}
}
