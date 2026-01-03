import { assert } from '$lib/asserts.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';

export interface OperatorMetrics {
	pushCalls: number;
	pushRowCounts: number[];
	emptyPushes: number;
	totalRowsPushed: number;
	pullCalls: number;
	totalRowsPulled: number;
	pullIterations: number[];
}
/**
 * Wraps an operator (Sink/Source) and tracks all operations
 */
/* stateless */
export class SpyOperator<T> implements Source<T> {
	#source: Source<T>;
	#sink: Sink<T> | typeof NullSink = NullSink;
	metrics: OperatorMetrics = {
		pushCalls: 0,
		pushRowCounts: [],
		emptyPushes: 0,
		totalRowsPushed: 0,
		pullCalls: 0,
		totalRowsPulled: 0,
		pullIterations: []
	};

	constructor(source: Source<T>) {
		this.#source = source;
		this.#source.setSink(this);
	}

	get size() {
		return this.#source.size;
	}

	push(changeSet: ChangeSet<T>): void {
		this.metrics.pushCalls++;
		const rowCount = changeSet.data.length;
		this.metrics.pushRowCounts.push(rowCount);
		this.metrics.totalRowsPushed += rowCount;

		// why are there empty pushes?
		if (changeSet.isEmpty()) {
			this.metrics.emptyPushes++;
		}

		this.#sink.push(changeSet);
	}

	*pull(): Generator<[T, number]> {
		this.metrics.pullCalls++;
		let rowCount = 0;
		try {
			for (const item of this.#source.pull()) {
				rowCount++;
				yield item;
			}
		} finally {
			this.metrics.totalRowsPulled += rowCount;
			this.metrics.pullIterations.push(rowCount);
		}
	}

	setSink(sink: Sink<T>): void {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	disconnect(): void {
		if ('disconnect' in this.#source) {
			this.#source.disconnect(this);
		}
	}

	// Get the underlying operator for further chaining
	unwrap(): Source<T> {
		return this.#source;
	}

	// Reset metrics for multiple test phases
	resetMetrics(): void {
		this.metrics = {
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		};
	}
}
