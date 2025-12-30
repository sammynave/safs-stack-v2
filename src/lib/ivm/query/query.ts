import type { ImmutableArray } from '$lib/immutable-types.js';
import {
	buildAggregations,
	buildDistinct,
	buildGroupBy,
	buildJoins,
	buildLeftJoins,
	buildLimit,
	buildOrderBy,
	buildOrderByComparator,
	buildProjection,
	buildWheres,
	universalComparator
} from './builder.ts';
import { View } from '../sinks/view.ts';
import { MapOperator } from '../stream-operators/map-operator.ts';
import { SplitStreamOperator } from '../stream-operators/split-stream-operator.ts';
import type { Source } from '../stream-operators/stream-operator-types.ts';
import type { AggregationDescriptor } from './aggregation.ts';

// eventually pass in a schema
type Database = Record<string, Source<unknown>>;

type SelectColumn = {
	column: string | AggregationDescriptor;
	alias?: string;
};

type WhereCondition =
	| { type: 'condition'; column: string; operator: string; value: any; conjunction: 'AND' | 'OR' }
	| { type: 'group'; conditions: WhereCondition[]; conjunction: 'AND' | 'OR' };
export class Query {
	static using(database: Database) {
		return new Query(database);
	}

	database;
	operations: {
		from: { type: 'from'; table: null | keyof Database };
		wheres: WhereCondition[];
		joins: {
			type: 'join';
			left: { value: string; table: keyof Database; column: string };
			right: { value: string; table: keyof Database; column: string };
		}[];
		leftJoins: {
			type: 'left-join';
			left: { value: string; table: keyof Database; column: string };
			right: { value: string; table: keyof Database; column: string };
		}[];
		select: { type: 'select'; columns: '*' | SelectColumn[] };
		orderBy: null | { type: 'orderBy'; columns: string[]; directions: ('asc' | 'desc')[] };
		requiresGroupBy: boolean;
		groupBy: { type: 'groupBy'; columns: string[] };
		limit: { type: 'limit'; num: number } | null;
		distinct: { type: 'distinct'; on: boolean; columns?: string[] } | null;
		with: null | { type: 'with'; tables: Record<string, unknown> };
	} = {
		from: { type: 'from', table: null },
		select: { type: 'select', columns: '*' },
		wheres: [],
		joins: [],
		leftJoins: [],
		orderBy: null,
		limit: null,
		requiresGroupBy: false,
		groupBy: { type: 'groupBy', columns: [] },
		distinct: null,
		with: null
	};

	// Core pipeline (built once, ends with SplitStreamOperator)
	#pipeline: Source<unknown> | null = null;
	#splitOperator: SplitStreamOperator<unknown> | null = null;

	// Single view for execute()/subscribe() - they share the same branch
	#view: View<unknown> | null = null;

	constructor(database: Database) {
		this.database = database;
	}

	with(tempTables) {
		const wrappedTempTables = Object.entries(tempTables).reduce((acc, [k, v]) => {
			const branch = v.#getNewBranch();
			// When a Query is used in .with(), get a NEW branch from it
			acc[k] = { connect: () => branch };
			return acc;
		}, {});
		this.operations.with = { type: 'with', tables: { ...this.database, ...wrappedTempTables } };
		return this;
	}

	select(columns: (string | AggregationDescriptor)[]) {
		// Parse columns to extract aliases
		const parsedColumns: SelectColumn[] = columns.map((col) => {
			if (typeof col === 'string') {
				// Handle string with ' as '
				const asIndex = col.toLowerCase().indexOf(' as ');
				if (asIndex !== -1) {
					return {
						column: col.substring(0, asIndex).trim(),
						alias: col.substring(asIndex + 4).trim()
					};
				}
				return { column: col };
			} else {
				// It's an AggregationDescriptor (possibly with 'alias' property)
				const alias = (col as any).alias;
				if (alias) {
					// Remove the alias property from the descriptor
					const { alias: _, as: __, ...aggDescriptor } = col as any;
					return { column: aggDescriptor, alias };
				}
				return { column: col };
			}
		});

		this.operations.select.columns = parsedColumns;

		const hasAggregations = parsedColumns.some(
			(col) => typeof col.column !== 'string' && col.column.type === 'aggregation'
		);

		const hasRegularColumns = parsedColumns.some((col) => typeof col.column === 'string');

		if (hasAggregations && hasRegularColumns) {
			this.operations.requiresGroupBy = true;
		} else if (hasAggregations) {
			this.operations.requiresGroupBy = false;
		}

		return this;
	}

	distinct() {
		this.operations.distinct = { type: 'distinct', on: false };
		return this;
	}

	distinctOn(columns: string[]) {
		this.operations.distinct = { type: 'distinct', on: true, columns };
		return this;
	}

	from(table: string) {
		this.operations.from.table = table;
		return this;
	}

	// TODO we ned to use `table` and make sure that `rightValue` is ALWAYS `table`
	join(table: keyof Database, leftValue: string, rightValue: string) {
		const [leftTable, leftColumn] = leftValue.split('.');
		const [rightTable, rightColumn] = rightValue.split('.');

		const positions =
			rightTable === table
				? {
						left: { value: leftValue, table: leftTable, column: leftColumn },
						right: { value: rightValue, table: rightTable, column: rightColumn }
					}
				: {
						left: { value: rightValue, table: rightTable, column: rightColumn },
						right: { value: leftValue, table: leftTable, column: leftColumn }
					};
		this.operations.joins.push({
			type: 'join',
			...positions
		});
		return this;
	}

	leftOuterJoin(table: keyof Database, leftValue: string, rightValue: string) {
		const [leftTable, leftColumn] = leftValue.split('.');
		const [rightTable, rightColumn] = rightValue.split('.');
		const positions =
			rightTable === table
				? {
						left: { value: leftValue, table: leftTable, column: leftColumn },
						right: { value: rightValue, table: rightTable, column: rightColumn }
					}
				: {
						left: { value: rightValue, table: rightTable, column: rightColumn },
						right: { value: leftValue, table: leftTable, column: leftColumn }
					};
		this.operations.leftJoins.push({
			type: 'left-join',
			...positions
		});
		return this;
	}

	leftJoinLateral(table: string, leftValue: string, rightValue: string) {
		throw Error('not implemented');
		return this;
	}

	innerJoinLateral(table: string, leftValue: string, rightValue: string) {
		throw Error('not implemented');
		return this;
	}

	crossJoin(table: string, leftValue: string, rightValue: string) {
		throw Error('not implemented');
		return this;
	}

	rightOuterJoin(table, leftValue: string, rightValue: string) {
		throw Error('not implemented');
		return this;
	}

	fullOuterJoin(table, leftValue: string, rightValue: string) {
		throw Error('not implemented');
		return this;
	}

	where(columnOrCallback, operator?, value?) {
		if (typeof columnOrCallback === 'function') {
			// Group case
			const subQuery = new Query(this.database);
			columnOrCallback(subQuery);
			this.operations.wheres.push({
				type: 'group',
				conditions: subQuery.operations.wheres,
				conjunction: 'AND'
			});
		} else {
			// Regular condition
			this.operations.wheres.push({
				type: 'condition',
				column: columnOrCallback,
				operator,
				value,
				conjunction: 'AND'
			});
		}
		return this;
	}

	and(columnOrCallback, operator?, value?) {
		return this.where(columnOrCallback, operator, value);
	}

	or(columnOrCallback, operator?, value?) {
		if (typeof columnOrCallback === 'function') {
			const subQuery = new Query(this.database);
			columnOrCallback(subQuery);
			this.operations.wheres.push({
				type: 'group',
				conditions: subQuery.operations.wheres,
				conjunction: 'OR'
			});
		} else {
			this.operations.wheres.push({
				type: 'condition',
				column: columnOrCallback,
				operator,
				value,
				conjunction: 'OR'
			});
		}
		return this;
	}

	groupBy(columns: string[]) {
		this.operations.groupBy.columns = columns;
		return this;
	}

	// TODO orderBy([['col1', 'desc'], ['col2, 'asc']]) might be a better api
	// than having multiple orderBys
	orderBy(column: string, direction: 'asc' | 'desc' = 'asc') {
		if (!this.operations.orderBy) {
			this.operations.orderBy = { type: 'orderBy', columns: [], directions: [] };
		}

		this.operations.orderBy.columns.push(column);
		this.operations.orderBy.directions.push(direction);
		return this;
	}

	limit(num: number) {
		this.operations.limit = { type: 'limit', num };
		return this;
	}

	/**
	 * Builds the core pipeline once, ending with a SplitStreamOperator.
	 * This allows the query to be reused multiple times.
	 */
	buildPipeline() {
		if (!this.#pipeline) {
			const withTables = this.operations.with ? this.operations.with.tables : this.database;

			// TODO can we use orderBy here as a perf trick?
			const source = withTables[this.operations.from.table].connect();
			const flattenedSource = new MapOperator(source, (row) => {
				const flattened = {};
				for (const [key, val] of Object.entries(row)) {
					flattened[`${this.operations.from.table}.${key}`] = val;
				}
				return flattened;
			});
			const joins = buildJoins(flattenedSource, this.operations.joins, withTables);
			const leftJoins = buildLeftJoins(joins, this.operations.leftJoins, withTables);
			const wheres = buildWheres(leftJoins, this.operations.wheres);

			if (this.operations.requiresGroupBy && this.operations.groupBy.columns.length === 0) {
				throw Error('Must have a .groupBy for non-aggregated columns');
			}
			const groupBy = buildGroupBy(wheres, this.operations.groupBy?.columns);
			const aggregations = buildAggregations(
				groupBy,
				this.operations.select.columns === '*'
					? []
					: this.operations.select.columns.filter(
							(col) => typeof col.column !== 'string' && col.column.type === 'aggregation'
						),
				this.operations.requiresGroupBy
			);

			const select = buildProjection(aggregations, this.operations.select.columns);
			const distinct = buildDistinct(
				select,
				this.operations.distinct,
				this.operations.select.columns === '*'
					? []
					: this.operations.select.columns.map(
							(col) =>
								col.alias ||
								(typeof col.column === 'string'
									? col.column
									: `${col.column.fn}(${col.column.column})`)
						)
			);
			const orderBy = buildOrderBy(distinct, this.operations.orderBy);
			const limit = buildLimit(orderBy, this.operations.limit, this.operations.orderBy);

			// NEW: End the pipeline with a SplitStreamOperator for reusability
			this.#splitOperator = new SplitStreamOperator(limit);
			// this.#pipeline = this.#splitOperator.branch();
		}

		// return this.#pipeline;
	}

	/**
	 * Internal method to get a NEW branch from the split operator.
	 * This is called when the query is used in .with() to ensure each
	 * use in a CTE gets its own independent branch.
	 */
	#getNewBranch(): Source<unknown> {
		// Ensure pipeline is built
		this.buildPipeline();

		// Return a new branch from the split operator
		return this.#splitOperator!.branch();
	}

	/**
	 * Internal method to get or create the shared branch for execute()/subscribe().
	 * Both methods use the same View on the same branch.
	 */
	#getOrCreateView(): View<unknown> {
		if (!this.#view) {
			// Get a branch for this query's execute/subscribe
			const branch = this.#getNewBranch();

			// Create a View on this branch
			const comparator = this.operations.orderBy
				? buildOrderByComparator(
						this.operations.orderBy.columns,
						this.operations.orderBy.directions
					)
				: universalComparator;

			this.#view = new View(branch, comparator);
		}

		return this.#view;
	}

	execute() {
		const view = this.#getOrCreateView();
		return view.materialize();
	}

	subscribe(cb: (results: ImmutableArray<unknown>) => void) {
		const view = this.#getOrCreateView();
		return view.subscribe(cb);
	}
}
