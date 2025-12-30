import { ArrayAggGroupByOperator } from '../stream-operators/aggregation/array-agg-group-by-operator.ts';
import { ArrayAggOperator } from '../stream-operators/aggregation/array-agg-operator.ts';
import { AvgGroupByOperator } from '../stream-operators/aggregation/avg-group-by-operator.ts';
import { AvgOperator } from '../stream-operators/aggregation/avg-operator.ts';
import { CountGroupByOperator } from '../stream-operators/aggregation/count-group-by-operator.ts';
import { CountOperator } from '../stream-operators/aggregation/count-operator.ts';
import { JsonAggGroupByOperator } from '../stream-operators/aggregation/json-agg-group-by-operator.ts';
import { JsonAggOperator } from '../stream-operators/aggregation/json-agg-operator.ts';
import { MaxGroupByOperator } from '../stream-operators/aggregation/max-group-by-operator.ts';
import { MaxOperator } from '../stream-operators/aggregation/max-opertator.ts';
import { MinGroupByOperator } from '../stream-operators/aggregation/min-group-by-operator.ts';
import { MinOperator } from '../stream-operators/aggregation/min-opertator.ts';
import { SumGroupByOperator } from '../stream-operators/aggregation/sum-group-by-operator.ts';
import { SumOperator } from '../stream-operators/aggregation/sum-operator.ts';
import { CombineOperator } from '../stream-operators/combine-operator.ts';
import { DistinctOperator } from '../stream-operators/distinct-operator.ts';
import { FilterOperator } from '../stream-operators/filter-operator.ts';
import { GroupByOperator } from '../stream-operators/group-by-operator.ts';
import { JoinOperator } from '../stream-operators/join-operator.ts';
import { LeftOuterJoinOperator } from '../stream-operators/left-outer-join-operator.ts';
import { LimitOperator } from '../stream-operators/limit-operator.ts';
import { MapOperator } from '../stream-operators/map-operator.ts';
import { MultiRowCombineOperator } from '../stream-operators/multi-row-combine-operator.ts';
import { OrderByOperator } from '../stream-operators/order-by-operator.ts';
import { ProjectOperator } from '../stream-operators/project-operator.ts';
import { SplitStreamOperator } from '../stream-operators/split-stream-operator.ts';

export function universalComparator<T>(a: T, b: T): number {
	const aStr = JSON.stringify(a);
	const bStr = JSON.stringify(b);
	return aStr.localeCompare(bStr);
}

export function buildLimit(
	source,
	limit: null | { type: 'limit'; num: number },
	orderBy: { type: 'orderBy'; columns: string[]; directions: ('asc' | 'desc')[] } | null
) {
	if (!limit) return source;
	const comparator = orderBy
		? buildOrderByComparator(orderBy.columns, orderBy.directions)
		: universalComparator;
	return new LimitOperator(source, limit.num, comparator);
}

export function buildJoins(source, joins, database) {
	if (joins.length === 0) return source;
	const result = joins.reduce((source, { left, right }) => {
		// Flatten the right source before joining
		const rawRightSource = database[right.table].connect();
		const flattenedRightSource = new MapOperator(rawRightSource, (row) => {
			const flattened = {};
			for (const [key, val] of Object.entries(row)) {
				flattened[`${right.table}.${key}`] = val;
			}
			return flattened;
		});

		// Both sides are now flattened, so use full column paths
		const leftKeyExtractor = (row) => row[left.value]; // e.g., row['users.id']
		const rightKeyExtractor = (row) => row[right.value]; // e.g., row['posts.userId']

		// Create the join operator
		const joinOperator = new JoinOperator(
			source,
			flattenedRightSource,
			leftKeyExtractor,
			rightKeyExtractor,
			universalComparator
		);

		// Flatten the join result - both sides are already flattened, just merge them
		const flattenFn = ([leftRow, rightRow]) => {
			// Both leftRow and rightRow are already flattened objects
			// Just merge them together
			return { ...leftRow, ...rightRow };
		};

		const flattenOperator = new MapOperator(joinOperator, flattenFn);

		return flattenOperator;
	}, source);

	return result;
}
export function buildLeftJoins(source, joins, database) {
	if (joins.length === 0) return source;
	const result = joins.reduce((source, { left, right }) => {
		// Track right-side columns as we see them
		let rightColumns: null | string[] = null;

		// Flatten the right source before joining
		const rawRightSource = database[right.table].connect();
		const flattenedRightSource = new MapOperator(rawRightSource, (row) => {
			const flattened = {};
			for (const [key, val] of Object.entries(row)) {
				flattened[`${right.table}.${key}`] = val;
			}
			// TODO when we have a schema and a query planner,
			// we can use the query planner to grab the columns
			// of a table or infer the columns and types (if it's a CTE)
			// Capture column names from first row we see
			if (rightColumns === null) {
				rightColumns = Object.keys(flattened);
			}
			return flattened;
		});

		// Both sides are now flattened, so use full column paths
		const leftKeyExtractor = (row) => row[left.value]; // e.g., row['users.id']
		const rightKeyExtractor = (row) => row[right.value]; // e.g., row['posts.userId']

		// Create the join operator
		const joinOperator = new LeftOuterJoinOperator(
			source,
			flattenedRightSource,
			leftKeyExtractor,
			rightKeyExtractor,
			universalComparator
		);

		// Flatten the join result - handle null rightRow by adding null columns
		const flattenFn = ([leftRow, rightRow]) => {
			if (rightRow === null) {
				// Create an object with all right-side columns set to null
				const nullRightRow = {};
				if (rightColumns !== null) {
					for (const col of rightColumns) {
						nullRightRow[col] = null;
					}
				}
				return { ...leftRow, ...nullRightRow };
			}
			// Capture columns if we haven't yet (in case first join result has non-null rightRow)
			if (rightColumns === null) {
				rightColumns = Object.keys(rightRow);
			}
			return { ...leftRow, ...rightRow };
		};

		const flattenOperator = new MapOperator(joinOperator, flattenFn);

		return flattenOperator;
	}, source);

	return result;
}

const filterOperationsMap = {
	'>': (col, val) => col > val,
	'<': (col, val) => col < val,
	'=': (col, val) => col === val
};

export function buildWheres(source, wheres) {
	if (wheres.length === 0) return source;

	return new FilterOperator(source, (row) => {
		return evaluateWhereConditions(row, wheres);
	});
}

/**
 * Recursively evaluates WHERE conditions with proper AND/OR precedence.
 * AND has higher precedence than OR (standard SQL behavior).
 *
 * Algorithm:
 * 1. Split conditions by OR into groups
 * 2. Within each group, all conditions are ANDed together
 * 3. Groups are ORed together
 * 4. Handle nested groups recursively
 */
function evaluateWhereConditions(row, conditions) {
	if (conditions.length === 0) return true;

	// Split into OR-separated groups (respecting AND precedence)
	const orGroups: (typeof conditions)[] = [];
	let currentAndGroup: typeof conditions = [];

	for (let i = 0; i < conditions.length; i++) {
		const condition = conditions[i];

		// First condition starts a group, OR starts a new group
		if (i === 0 || condition.conjunction === 'AND') {
			currentAndGroup.push(condition);
		} else {
			// conjunction === 'OR'
			// Finish current AND group and start new one
			if (currentAndGroup.length > 0) {
				orGroups.push(currentAndGroup);
			}
			currentAndGroup = [condition];
		}
	}

	// Don't forget the last group
	if (currentAndGroup.length > 0) {
		orGroups.push(currentAndGroup);
	}

	// OR the groups together: at least one group must be true
	return orGroups.some((andGroup) => {
		// AND all conditions within the group: all must be true
		return andGroup.every((condition) => {
			if (condition.type === 'group') {
				// Recursively evaluate nested group
				return evaluateWhereConditions(row, condition.conditions);
			} else {
				// Evaluate single condition
				const { column, operator, value } = condition;
				return filterOperationsMap[operator](row[column], value);
			}
		});
	});
}

export function buildProjection(source, columns) {
	if (!columns || columns === '*') return source;
	return new ProjectOperator(source, {
		columns: columns.reduce((acc, selectCol) => {
			const { column, alias } = selectCol;

			if (typeof column !== 'string' && column.type === 'aggregation') {
				// Aggregation
				const defaultKey = `${column.fn}(${column.column})`;
				const outputKey = alias || defaultKey;
				acc[outputKey] = (row) => {
					return row[column.fn];
				};
			} else {
				// Regular column
				const outputKey = alias || column;
				acc[outputKey] = (row) => {
					if (!(column in row)) {
						throw new Error(
							`Column '${column}' does not exist in row. Available columns: ${Object.keys(row).join(', ')}`
						);
					}
					return row[column];
				};
			}

			return acc;
		}, {})
	});
}

export function buildAggregations(source, aggregations, requiresGroupBy) {
	// Extract just the aggregation descriptors from SelectColumn objects
	const aggDescriptors = aggregations.map((col) => col.column);

	if (aggDescriptors.length === 0) return source;

	const split = new SplitStreamOperator(source);

	const operators = requiresGroupBy
		? groupByAggregations(split, aggDescriptors)
		: nonGroupByAggregations(split, aggDescriptors);

	const CombinerClass = requiresGroupBy ? MultiRowCombineOperator : CombineOperator;

	let combined = operators[0];
	for (let i = 1; i < operators.length; i++) {
		combined = new CombinerClass(combined, operators[i], (left, right) => ({
			...left,
			...right
		}));
	}
	return combined;
}

function nonGroupByAggregations(split, aggregations) {
	return aggregations.map((agg) => {
		const branch = split.branch();
		switch (agg.fn) {
			case 'count':
				return new CountOperator(branch, agg.column === '*' ? {} : { column: agg.column });
			case 'sum':
				return new SumOperator(branch, { column: agg.column });
			case 'avg':
				return new AvgOperator(branch, { column: agg.column });
			case 'max':
				return new MaxOperator(branch, { column: agg.column });
			case 'min':
				return new MinOperator(branch, { column: agg.column });
			case 'arrayAgg':
				return new ArrayAggOperator(branch, { column: agg.column });
			case 'jsonAgg':
				return new JsonAggOperator(branch, { column: agg.column });
			default:
				throw Error(`Unknown agg function ${agg.fn}: ${agg}`);
		}
	});
}
function groupByAggregations(split, aggregations) {
	return aggregations.map((agg) => {
		const branch = split.branch();
		switch (agg.fn) {
			case 'count':
				return new CountGroupByOperator(branch);
			case 'sum':
				return new SumGroupByOperator(branch, agg.column);
			case 'avg':
				return new AvgGroupByOperator(branch, agg.column);
			case 'max':
				return new MaxGroupByOperator(branch, agg.column);
			case 'min':
				return new MinGroupByOperator(branch, agg.column);
			case 'arrayAgg':
				return new ArrayAggGroupByOperator(branch, agg.column);
			case 'jsonAgg':
				return new JsonAggGroupByOperator(branch, agg.column);
			default:
				throw Error(`Unknown agg function ${agg.fn}: ${agg}`);
		}
	});
}

export function buildGroupBy(source, columns) {
	if (columns.length === 0) return source;
	return new GroupByOperator(source, columns, universalComparator);
}

export function buildDistinct(source, distinct, columns) {
	if (!distinct) return source;
	const comparator = distinct.on
		? buildDistinctComparatorFrom(distinct.columns)
		: buildDistinctComparatorFrom(columns);

	return new DistinctOperator(source, comparator);
}

function buildDistinctComparatorFrom(columns: string[]) {
	return (a: Record<string, unknown>, b: Record<string, unknown>): number => {
		// Compare each column in order
		for (const column of columns) {
			const aVal = a[column];
			const bVal = b[column];

			// Handle equality first
			if (aVal === bVal) continue;

			// Handle null/undefined
			if (aVal === null || typeof aVal === 'undefined') return -1;
			if (bVal === null || typeof bVal === 'undefined') return 1;

			// Handle booleans
			if (typeof aVal === 'boolean' && typeof bVal === 'boolean') {
				return aVal ? 1 : -1;
			}

			// Handle numbers
			if (typeof aVal === 'number' && typeof bVal === 'number') {
				return aVal - bVal;
			}

			// Handle strings
			if (typeof aVal === 'string' && typeof bVal === 'string') {
				return aVal.localeCompare(bVal);
			}

			// Fallback for unsupported types
			throw Error(
				`typeof a[${column}]: ${typeof aVal} and/or typeof b[${column}]: ${typeof bVal} not supported`
			);
		}

		// All columns are equal
		return 0;
	};
}

export function buildOrderBy(
	source,
	orderBy: { type: 'orderBy'; columns: string[]; directions: ('asc' | 'desc')[] } | null
) {
	if (!orderBy) return source;

	const comparator = buildOrderByComparator(orderBy.columns, orderBy.directions);
	return new OrderByOperator(source, comparator);
}

export function buildOrderByComparator(columns: string[], directions: ('asc' | 'desc')[]) {
	return (a: Record<string, unknown>, b: Record<string, unknown>): number => {
		// First, compare ORDER BY columns
		for (let i = 0; i < columns.length; i++) {
			const column = columns[i];
			const direction = directions[i] || 'asc';
			const aVal = a[column];
			const bVal = b[column];

			if (aVal === bVal) continue;

			// Handle nulls
			if (aVal === null || aVal === undefined) return direction === 'desc' ? 1 : -1;
			if (bVal === null || bVal === undefined) return direction === 'desc' ? -1 : 1;

			// Compare values
			let result = 0;
			if (typeof aVal === 'number' && typeof bVal === 'number') {
				result = aVal - bVal;
			} else if (typeof aVal === 'string' && typeof bVal === 'string') {
				result = aVal.localeCompare(bVal);
			} else if (typeof aVal === 'boolean' && typeof bVal === 'boolean') {
				result = aVal ? 1 : -1;
			} else {
				// Fallback for other types
				result = String(aVal).localeCompare(String(bVal));
			}

			// Apply direction and return immediately if not equal
			return direction === 'desc' ? -result : result;
		}

		// TIEBREAKER: All ORDER BY columns are equal
		// Use JSON stringify to ensure uniqueness (prevents BTree from losing rows)
		const aStr = JSON.stringify(a);
		const bStr = JSON.stringify(b);
		return aStr.localeCompare(bStr);
	};
}
