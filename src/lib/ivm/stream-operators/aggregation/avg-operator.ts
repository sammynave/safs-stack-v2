import { assert } from '$lib/asserts.js';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';
import { SumOperator } from './sum-operator.ts';
import { CountOperator } from './count-operator.ts';
import { SplitStreamOperator } from '../split-stream-operator.ts';
import { CombineOperator } from '../combine-operator.ts';

interface AvgOptions<T> {
	column: keyof T;
}

type AvgResult = { avg: number | null };

/**
 * AvgOperator computes the average of a column by composing SumOperator and CountOperator.
 *
 * Architecture:
 *              Source<T>
 *                  |
 *             SplitStreamOperator
 *              /       \
 *      SumOperator   CountOperator
 *              \       /
 *           MergeOperator
 *                  |
 *            {avg: sum/count}
 */
export class AvgOperator<T> implements Source<AvgResult> {
	readonly #mergeOperator: CombineOperator<{ sum: number }, { count: number }, AvgResult>;
	#sink: Sink<AvgResult> | typeof NullSink = NullSink;

	constructor(source: Source<T>, options: AvgOptions<T>) {
		// Split the source into two branches
		const split = new SplitStreamOperator(source);
		const branch1 = split.branch();
		const branch2 = split.branch();

		// Create sum and count operators on separate branches
		const sumOp = new SumOperator(branch1, { column: options.column });
		const countOp = new CountOperator(branch2, { column: options.column });

		// Merge their results to compute average
		this.#mergeOperator = new CombineOperator(sumOp, countOp, ({ sum }, { count }) => ({
			avg: count > 0 ? sum / count : null
		}));
	}

	get size() {
		return this.#mergeOperator.size;
	}

	get avg(): number | null {
		for (const [result] of this.#mergeOperator.pull()) {
			return result.avg;
		}
		return null;
	}

	setSink(sink: Sink<AvgResult>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
		this.#mergeOperator.setSink(sink);
	}

	*pull() {
		yield* this.#mergeOperator.pull();
	}

	disconnect() {
		this.#mergeOperator.disconnect();
	}
}
