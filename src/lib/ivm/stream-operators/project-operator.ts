import { assert } from '$lib/asserts.js';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';
import type { Comparator } from '../b-plus-tree/tree.ts';

/**
 * Column definition: maps output column names to transformation functions
 */
type ColumnDefinitions<TIn, TOut> = {
	[K in keyof TOut]: (row: TIn) => TOut[K];
};

/**
 * Configuration for ProjectOperator
 */
type ProjectConfig<TIn, TOut> = {
	columns: ColumnDefinitions<TIn, TOut>;
	comparator?: Comparator<TOut>;
};

/**
 * ProjectOperator - Transforms row shape (SQL SELECT clause)
 *
 * Stateless operator that:
 * - Projects columns (selects subset of fields)
 * - Computes derived columns (expressions)
 * - Renames columns
 * - Adds constants
 *
 * @example
 * ```typescript
 * const project = new ProjectOperator(orders, {
 *   columns: {
 *     userId: (row) => row.userId,
 *     doubled: (row) => row.amount * 2,
 *     status: () => 'ACTIVE'
 *   },
 *   comparator: (a, b) => a.userId - b.userId
 * });
 * ```
 */
export class ProjectOperator<TIn, TOut> implements Sink<TIn>, Source<TOut> {
	readonly #source: Source<TIn>;
	readonly #columns: ColumnDefinitions<TIn, TOut>;
	readonly #comparator?: Comparator<TOut>;
	#sink: Sink<TOut> | typeof NullSink = NullSink;

	constructor(source: Source<TIn>, config: ProjectConfig<TIn, TOut>) {
		this.#source = source;
		this.#columns = config.columns;
		this.#comparator = config.comparator;
		this.#source.setSink(this);
	}

	get size() {
		return this.#source.size;
	}

	setSink(sink: Sink<TOut>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push(changeSet: ChangeSet<TIn>) {
		// Transform each row in the changeset
		const transformedData: [TOut, number][] = [];

		for (const [row, weight] of changeSet.data) {
			const transformedRow = this.#transformRow(row);
			transformedData.push([transformedRow, weight]);
		}

		const outputChangeSet = new ChangeSet<TOut>(transformedData);
		this.#sink.push(outputChangeSet);
	}

	*pull() {
		for (const [row, weight] of this.#source.pull()) {
			const transformedRow = this.#transformRow(row);
			yield [transformedRow, weight] as [TOut, number];
		}
	}

	disconnect() {
		this.#source.disconnect(this);
	}

	/**
	 * Transform a single row by applying all column definitions
	 */

	#transformRow(inputRow: TIn): TOut {
		const outputRow = {} as TOut;

		// Apply each column transformation
		for (const columnName in this.#columns) {
			if (Object.hasOwn(this.#columns, columnName)) {
				const transformer = this.#columns[columnName];
				outputRow[columnName] = transformer(inputRow);
			}
		}

		return outputRow;
	}

	/**
	 * Get the comparator for downstream operators
	 * Returns undefined if no comparator was provided
	 */
	get comparator(): Comparator<TOut> | undefined {
		return this.#comparator;
	}
}
